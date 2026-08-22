//! Motor MERGE/upsert -- reemplazo de `BaseTransferRepository`. Feature
//! `tablesync` (default on): se saca la feature y queda una libreria ODBC
//! pura, sin esto (ver AGENTS.md ss2).
//!
//! Expuesto solo por `Db2iEngine.table_sync(source=...)`, por composicion,
//! NUNCA por herencia. Las clases del consumidor pasan a *tener* un
//! `TableSync` en vez de heredar de el.
//!
//! Simplificacion deliberada de esta primera pasada (ver AGENTS.md ss9 y
//! `tablesync/ddl.rs`): el `USING (VALUES ...)` se arma directo en el
//! statement `MERGE`, sin tabla temporal de sesion. `merge_chunk_size`
//! limita filas por statement.

pub mod catalog;
pub mod ddl;
pub mod merge;

use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::core::{ParamValue, SharedEngine};
use crate::errors::to_py_err;
use crate::params::param_value_from_python;

#[pyclass(module = "rustodbc")]
#[derive(Debug, Clone)]
pub struct MergeReport {
    #[pyo3(get)]
    pub rows_affected: i64,
    #[pyo3(get)]
    pub batches: usize,
    #[pyo3(get)]
    pub used_merge: bool,
    #[pyo3(get)]
    pub warning: Option<String>,
}

#[pymethods]
impl MergeReport {
    fn __repr__(&self) -> String {
        format!(
            "MergeReport(rows_affected={}, batches={}, used_merge={}, warning={:?})",
            self.rows_affected, self.batches, self.used_merge, self.warning
        )
    }
}

#[pyclass(module = "rustodbc")]
pub struct TableSync {
    dest: SharedEngine,
    #[allow(dead_code)]
    source: Option<SharedEngine>,
    merge_chunk_size: usize,
    /// Runtime tokio para `merge_sync` (solo presente en la fachada
    /// `BlockingEngine`). La fachada async usa `merge()` (awaitable).
    runtime: Option<tokio::runtime::Handle>,
}

impl TableSync {
    pub fn new(
        dest: SharedEngine,
        source: Option<SharedEngine>,
        merge_chunk_size: usize,
        runtime: Option<tokio::runtime::Handle>,
    ) -> Self {
        TableSync {
            dest,
            source,
            merge_chunk_size,
            runtime,
        }
    }
}

fn records_to_columns_and_rows(
    py: Python<'_>,
    records: &Bound<'_, PyAny>,
) -> PyResult<(Vec<String>, Vec<Vec<ParamValue>>)> {
    let mut columns: Option<Vec<String>> = None;
    let mut rows = Vec::new();

    for item in records.iter()? {
        let item = item?;
        let dict = item.downcast::<PyDict>().map_err(|_| {
            to_py_err(crate::errors::CoreError::Parameter(
                "merge(): cada record debe ser un dict".to_string(),
            ))
        })?;

        let cols: Vec<String> = match &columns {
            Some(c) => c.clone(),
            None => {
                let c: Vec<String> = dict
                    .keys()
                    .iter()
                    .map(|k| k.extract::<String>())
                    .collect::<PyResult<_>>()?;
                columns = Some(c.clone());
                c
            }
        };

        let mut row = Vec::with_capacity(cols.len());
        for col in &cols {
            let value = dict.get_item(col)?.ok_or_else(|| {
                to_py_err(crate::errors::CoreError::Parameter(format!(
                    "merge(): falta la columna {col:?} en un record -- todos los records deben \
                     tener las mismas keys"
                )))
            })?;
            row.push(param_value_from_python(py, &value)?);
        }
        rows.push(row);
    }

    Ok((columns.unwrap_or_default(), rows))
}

#[pymethods]
impl TableSync {
    #[pyo3(signature = (schema, table, records, primary_key=None))]
    fn merge<'py>(
        &self,
        py: Python<'py>,
        schema: String,
        table: String,
        records: Bound<'py, PyAny>,
        primary_key: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let (columns, rows) = records_to_columns_and_rows(py, &records)?;
        let dest = self.dest.clone();
        let chunk_size = self.merge_chunk_size;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let lease = dest.acquire().await.map_err(to_py_err)?;
            tokio::task::spawn_blocking(move || {
                merge_report_sync(
                    &lease,
                    &schema,
                    &table,
                    &columns,
                    &rows,
                    chunk_size,
                    primary_key,
                )
            })
            .await
            .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
        })
    }

    /// Variante sincrona del MERGE (fachada `BlockingEngine`). Requiere que
    /// el `TableSync` se haya creado con un runtime tokio (via
    /// `BlockingEngine.table_sync()`); si no, `InterfaceError`.
    #[pyo3(signature = (schema, table, records, primary_key=None))]
    fn merge_sync(
        &self,
        py: Python<'_>,
        schema: String,
        table: String,
        records: Bound<'_, PyAny>,
        primary_key: Option<Vec<String>>,
    ) -> PyResult<MergeReport> {
        let (columns, rows) = records_to_columns_and_rows(py, &records)?;
        let runtime = self.runtime.clone().ok_or_else(|| {
            to_py_err(crate::errors::CoreError::Interface(
                "merge_sync solo esta disponible via BlockingEngine.table_sync()".to_string(),
            ))
        })?;
        let dest = self.dest.clone();
        let chunk_size = self.merge_chunk_size;

        py.allow_threads(move || {
            let lease = runtime
                .block_on(async move { dest.acquire().await })
                .map_err(to_py_err)?;
            merge_report_sync(
                &lease,
                &schema,
                &table,
                &columns,
                &rows,
                chunk_size,
                primary_key,
            )
        })
    }
}

/// Cuerpo compartido del MERGE (async y sync): resuelve PK, degrada a INSERT
/// sin PK, y ejecuta los chunks.
fn merge_report_sync(
    lease: &crate::core::Lease,
    schema: &str,
    table: &str,
    columns: &[String],
    rows: &[Vec<ParamValue>],
    chunk_size: usize,
    primary_key: Option<Vec<String>>,
) -> PyResult<MergeReport> {
    if rows.is_empty() || columns.is_empty() {
        return Ok(MergeReport {
            rows_affected: 0,
            batches: 0,
            used_merge: false,
            warning: Some("merge(): records vacio, nada para hacer".to_string()),
        });
    }

    let pk_columns = match primary_key {
        Some(pk) => pk,
        None => catalog::primary_key_columns(lease, schema, table).map_err(to_py_err)?,
    };

    if pk_columns.is_empty() {
        // Regla dura AGENTS.md ss4: sin PK, INSERT con warning,
        // nunca crash y nunca MERGE silencioso sin clave.
        let (rows_affected, batches) =
            merge::insert_only_rows(lease, schema, table, columns, rows, chunk_size)
                .map_err(to_py_err)?;
        return Ok(MergeReport {
            rows_affected,
            batches,
            used_merge: false,
            warning: Some(format!(
                "{schema}.{table} no tiene PK/indice unico en el catalogo -- se hizo INSERT \
                 simple, no MERGE"
            )),
        });
    }

    let (rows_affected, batches) =
        merge::merge_rows(lease, schema, table, &pk_columns, columns, rows, chunk_size)
            .map_err(to_py_err)?;

    Ok(MergeReport {
        rows_affected,
        batches,
        used_merge: true,
        warning: None,
    })
}

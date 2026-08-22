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

use crate::core::ffi::ColumnMeta;
use crate::core::{ColumnValue, ParamValue, SharedEngine};
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
    /// Engine origen para `transfer()`. `None` si el `TableSync` se creo sin
    /// `source` (entonces `transfer` no esta disponible).
    source: Option<SharedEngine>,
    merge_chunk_size: usize,
    /// Runtime tokio para `merge_sync`/`transfer_sync` (solo presente en la
    /// fachada `BlockingEngine`). La fachada async usa `merge()` (awaitable).
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
        let limits = dest.limits.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let lease = dest.acquire().await.map_err(to_py_err)?;
            tokio::task::spawn_blocking(move || {
                merge_report_sync(
                    &lease,
                    &limits,
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
        let limits = dest.limits.clone();

        py.allow_threads(move || {
            let lease = runtime
                .block_on(async move { dest.acquire().await })
                .map_err(to_py_err)?;
            merge_report_sync(
                &lease,
                &limits,
                &schema,
                &table,
                &columns,
                &rows,
                chunk_size,
                primary_key,
            )
        })
    }

    /// Copia `schema.table` desde el engine `source` al engine `dest`
    /// (`select_sql` por defecto: `SELECT * FROM schema.table`; permite un
    /// SELECT con filtro). Lee en streaming (RAM acotada por lote) y escribe
    /// con MERGE (o INSERT si no hay PK). `select_sql` debe devolver las
    /// mismas columnas que la tabla destino.
    #[pyo3(signature = (schema, table, *, select_sql=None, primary_key=None))]
    fn transfer<'py>(
        &self,
        py: Python<'py>,
        schema: String,
        table: String,
        select_sql: Option<String>,
        primary_key: Option<Vec<String>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let source = self.source.clone().ok_or_else(|| {
            to_py_err(crate::errors::CoreError::Interface(
                "transfer() requiere que table_sync(source=...) se cree con un engine origen"
                    .to_string(),
            ))
        })?;
        let dest = self.dest.clone();
        let chunk_size = self.merge_chunk_size;
        let select_sql = select_sql.unwrap_or_else(|| format!("SELECT * FROM {schema}.{table}"));
        let limits = dest.limits.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let src_lease = source.acquire().await.map_err(to_py_err)?;
            let dest_lease = dest.acquire().await.map_err(to_py_err)?;
            tokio::task::spawn_blocking(move || {
                transfer_report_sync(
                    &src_lease,
                    &dest_lease,
                    &limits,
                    &select_sql,
                    &schema,
                    &table,
                    chunk_size,
                    primary_key,
                )
            })
            .await
            .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
        })
    }

    /// Variante sincrona de `transfer` (fachada `BlockingEngine`).
    #[pyo3(signature = (schema, table, *, select_sql=None, primary_key=None))]
    fn transfer_sync(
        &self,
        py: Python<'_>,
        schema: String,
        table: String,
        select_sql: Option<String>,
        primary_key: Option<Vec<String>>,
    ) -> PyResult<MergeReport> {
        let source = self.source.clone().ok_or_else(|| {
            to_py_err(crate::errors::CoreError::Interface(
                "transfer_sync() requiere que table_sync(source=...) se cree con un engine origen"
                    .to_string(),
            ))
        })?;
        let runtime = self.runtime.clone().ok_or_else(|| {
            to_py_err(crate::errors::CoreError::Interface(
                "transfer_sync solo esta disponible via BlockingEngine.table_sync()".to_string(),
            ))
        })?;
        let dest = self.dest.clone();
        let chunk_size = self.merge_chunk_size;
        let select_sql = select_sql.unwrap_or_else(|| format!("SELECT * FROM {schema}.{table}"));
        let limits = dest.limits.clone();

        py.allow_threads(move || {
            let src_lease = runtime
                .block_on(async move { source.acquire().await })
                .map_err(to_py_err)?;
            let dest_lease = runtime
                .block_on(async move { dest.acquire().await })
                .map_err(to_py_err)?;
            transfer_report_sync(
                &src_lease,
                &dest_lease,
                &limits,
                &select_sql,
                &schema,
                &table,
                chunk_size,
                primary_key,
            )
        })
    }
}

/// Cuerpo compartido del MERGE (async y sync): resuelve PK, degrada a INSERT
/// sin PK, y ejecuta los chunks (con halve-and-retry de chunk size).
fn merge_report_sync(
    lease: &crate::core::Lease,
    limits: &std::sync::Mutex<crate::core::StatementLimits>,
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
            merge::insert_only_rows(lease, limits, schema, table, columns, rows, chunk_size)
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

    let (rows_affected, batches) = merge::merge_rows(
        lease,
        limits,
        schema,
        table,
        &pk_columns,
        columns,
        rows,
        chunk_size,
    )
    .map_err(to_py_err)?;

    Ok(MergeReport {
        rows_affected,
        batches,
        used_merge: true,
        warning: None,
    })
}

/// Lee `select_sql` desde `src_lease` en streaming (lotes de `chunk_size`),
/// convierte cada lote a `Vec<Vec<ParamValue>>` con las columnas del SELECT y
/// lo mergea/inserta en `dest_lease`. RAM acotada por lote.
fn transfer_report_sync(
    src_lease: &crate::core::Lease,
    dest_lease: &crate::core::Lease,
    limits: &std::sync::Mutex<crate::core::StatementLimits>,
    select_sql: &str,
    schema: &str,
    table: &str,
    chunk_size: usize,
    primary_key: Option<Vec<String>>,
) -> PyResult<MergeReport> {
    let mut cursor = src_lease.query_cursor(select_sql, &[]).map_err(to_py_err)?;
    let columns_meta: Vec<ColumnMeta> = cursor.column_metas();
    if columns_meta.is_empty() {
        return Err(to_py_err(crate::errors::CoreError::Query {
            sqlstate: "42S02".to_string(),
            native_code: 0,
            message: format!("transfer(): el SELECT no devolvio columnas: {select_sql}"),
            diagnostics: Vec::new(),
        }));
    }
    let columns: Vec<String> = columns_meta.iter().map(|c| c.name.clone()).collect();

    let pk_columns = match primary_key {
        Some(pk) => pk,
        None => catalog::primary_key_columns(dest_lease, schema, table).map_err(to_py_err)?,
    };
    let use_merge = !pk_columns.is_empty();

    let mut total = 0i64;
    let mut batches = 0usize;

    loop {
        let batch = cursor.fetch_batch(chunk_size).map_err(to_py_err)?;
        if batch.is_empty() {
            break;
        }
        // Convertir ColumnValue -> ParamValue (mismo orden que columns).
        let rows: Vec<Vec<ParamValue>> = batch
            .iter()
            .map(|row| {
                row.iter()
                    .map(|v| match v {
                        ColumnValue::Null => ParamValue::Null,
                        ColumnValue::Text(s) => ParamValue::Text(s.clone()),
                        ColumnValue::Binary(b) => ParamValue::Bytes(b.clone()),
                    })
                    .collect()
            })
            .collect();

        let (affected, chunks) = if use_merge {
            merge::merge_rows(
                dest_lease,
                limits,
                schema,
                table,
                &pk_columns,
                &columns,
                &rows,
                chunk_size,
            )
            .map_err(to_py_err)?
        } else {
            merge::insert_only_rows(
                dest_lease, limits, schema, table, &columns, &rows, chunk_size,
            )
            .map_err(to_py_err)?
        };
        total += affected;
        batches += chunks;
    }

    Ok(MergeReport {
        rows_affected: total,
        batches,
        used_merge: use_merge,
        warning: if use_merge {
            None
        } else {
            Some(format!(
                "{schema}.{table} no tiene PK/indice unico en el catalogo -- se hizo INSERT \
                 simple, no MERGE"
            ))
        },
    })
}

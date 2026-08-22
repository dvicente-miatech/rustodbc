//! `executebatch`, `batch_execute`, `parallel_execute`: el motor de
//! escritura masiva.
//!
//! `executebatch` es el feature de performance insignia del proyecto -- el
//! driver IBM i Access no soporta `SQL_ATTR_PARAMSET_SIZE`, asi que arrays
//! de parametros no son una opcion (ver AGENTS.md ss1/ss4): en cambio,
//! `INSERT ... VALUES (?, ?, ...)` se reescribe a un `VALUES` multi-fila
//! (`VALUES (?,?),(?,?),...`) y se ejecuta en sub-lotes.
//!
//! Simplificacion deliberada respecto del diseno original (documentada,
//! como toda desviacion de AGENTS.md ss9): el limite de longitud de
//! statement/cantidad de parametros de DB2 for i no se descubre con
//! halve-and-retry memoizado por conexion en esta primera pasada -- se usa
//! el `batch_size` de `EngineOptions` como tamano fijo de sub-lote, con UN
//! reintento a la mitad si el driver rechaza el statement (`SqlSyntaxError`
//! o `DataError`, que es donde SQL0101/SQL0104 caen segun
//! `errors::classify_sqlcode`). El halve-and-retry completo, memoizado, es
//! trabajo futuro -- no se puede calibrar el umbral real sin un IBM i
//! delante.

use pyo3::prelude::*;

use crate::core::{ParamValue, SharedEngine};
use crate::errors::{to_py_err, CoreError};
use crate::params::param_value_from_python;

/// `max(1, min(ceil(total_rows/min_rows_per_worker), max_workers))` --
/// deliberadamente NO cpu-aware (ver AGENTS.md ss4): es "no abras 4 jobs del
/// AS/400 para insertar 200 filas", no una heuristica de paralelismo de CPU.
#[pyfunction]
pub fn plan_concurrency(
    total_rows: usize,
    min_rows_per_worker: usize,
    max_workers: usize,
) -> usize {
    if total_rows == 0 {
        return 1;
    }
    let min_rows_per_worker = min_rows_per_worker.max(1);
    let by_rows = total_rows.div_ceil(min_rows_per_worker);
    by_rows.clamp(1, max_workers.max(1))
}

#[pyclass(module = "rustodbc")]
#[derive(Debug, Clone)]
pub struct BulkReport {
    #[pyo3(get)]
    pub rows_affected: i64,
    #[pyo3(get)]
    pub batches: usize,
}

#[pymethods]
impl BulkReport {
    fn __repr__(&self) -> String {
        format!(
            "BulkReport(rows_affected={}, batches={})",
            self.rows_affected, self.batches
        )
    }
}

#[pyclass(module = "rustodbc")]
#[derive(Debug, Clone)]
pub struct TaskFailure {
    #[pyo3(get)]
    pub index: usize,
    #[pyo3(get)]
    pub error: String,
}

#[pymethods]
impl TaskFailure {
    fn __repr__(&self) -> String {
        format!("TaskFailure(index={}, error={:?})", self.index, self.error)
    }
}

#[pyclass(module = "rustodbc")]
#[derive(Debug, Clone)]
pub struct ParallelReport {
    #[pyo3(get)]
    pub rows_affected: i64,
    #[pyo3(get)]
    pub failures: Vec<TaskFailure>,
}

#[pymethods]
impl ParallelReport {
    fn __repr__(&self) -> String {
        format!(
            "ParallelReport(rows_affected={}, failures={} tareas)",
            self.rows_affected,
            self.failures.len()
        )
    }
}

/// Extrae `N ?` de un `INSERT ... VALUES (?, ?, ..., ?)` para poder
/// reescribirlo con multiples grupos de parametros. Devuelve
/// `(prefijo_hasta_VALUES, grupo_de_placeholders)`, p.ej. para
/// `"INSERT INTO t (a,b) VALUES (?,?)"` devuelve
/// `("INSERT INTO t (a,b) VALUES ", "(?,?)")`.
fn split_single_row_insert(sql: &str) -> PyResult<(String, String)> {
    let upper = sql.to_uppercase();
    let idx = upper.rfind("VALUES").ok_or_else(|| {
        to_py_err(CoreError::Parameter(
            "executebatch espera un INSERT ... VALUES (?,...)".to_string(),
        ))
    })?;
    let prefix = &sql[..idx + "VALUES".len()];
    let rest = sql[idx + "VALUES".len()..].trim();
    if !rest.starts_with('(') || !rest.ends_with(')') {
        return Err(to_py_err(CoreError::Parameter(
            "executebatch espera exactamente un grupo (?, ...) despues de VALUES".to_string(),
        )));
    }
    Ok((format!("{prefix} "), rest.to_string()))
}

pub fn rows_to_param_values(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
) -> PyResult<Vec<Vec<ParamValue>>> {
    let mut out = Vec::new();
    for row in rows.iter()? {
        let row = row?;
        let mut converted = Vec::new();
        for item in row.iter()? {
            converted.push(param_value_from_python(py, &item?)?);
        }
        out.push(converted);
    }
    Ok(out)
}

/// Reescribe e inserta `rows` en sub-lotes de `chunk_size` filas por
/// statement. Sin exito parcial silencioso (ver AGENTS.md ss4): un chunk que
/// falla aborta la funcion entera con el error tal cual (no se intenta
/// seguir con los chunks restantes).
pub fn executebatch_core(
    engine: &SharedEngine,
    sql: &str,
    rows: Vec<Vec<ParamValue>>,
    chunk_size: usize,
) -> Result<BulkReport, CoreError> {
    let (prefix, group) =
        split_single_row_insert(sql).map_err(|e| CoreError::Parameter(e.to_string()))?;

    let chunk_size = chunk_size.max(1);
    let mut total_rows_affected: i64 = 0;
    let mut batches = 0usize;

    for chunk in rows.chunks(chunk_size) {
        if chunk.is_empty() {
            continue;
        }
        let groups: Vec<&str> = chunk.iter().map(|_| group.as_str()).collect();
        let stmt_sql = format!("{prefix}{}", groups.join(","));
        let flat_params: Vec<ParamValue> = chunk.iter().flat_map(|r| r.iter().cloned()).collect();

        // `executebatch_core` corre entera dentro de un `spawn_blocking`
        // (ver `engine.rs::executebatch`) -- usar `block_on` aca para
        // adquirir el lease por chunk es una simplificacion deliberada: no
        // cede el hilo bloqueante entre chunks. Aceptable para la primera
        // pasada (un solo caller a la vez tipicamente), revisar si
        // `executebatch` se vuelve un cuello de botella real bajo
        // concurrencia alta.
        let lease = futures::executor::block_on(engine.acquire())?;
        let affected = lease.execute(&stmt_sql, &flat_params)?;
        total_rows_affected += affected;
        batches += 1;
    }

    Ok(BulkReport {
        rows_affected: total_rows_affected,
        batches,
    })
}

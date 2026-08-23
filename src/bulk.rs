//! `executebatch`, `batch_execute`, `parallel_execute`: el motor de
//! escritura masiva.
//!
//! `executebatch` es el feature de performance insignia del proyecto -- el
//! driver IBM i Access no soporta `SQL_ATTR_PARAMSET_SIZE`, asi que arrays
//! de parametros no son una opcion (ver AGENTS.md ss1/ss4): en cambio,
//! `INSERT ... VALUES (?, ?, ...)` se reescribe a un `VALUES` multi-fila
//! (`VALUES (?,?),(?,?),...`) y se ejecuta en sub-lotes.
//!
//! Los limites de longitud de statement/cantidad de parametros de DB2 for i
//! (SQL0101/SQL54001) se descubren con halve-and-retry memoizado por engine
//! (ver `core::StatementLimits`): el primer chunk que excede se reduce a la
//! mitad y se reintenta; el tamano que funciono queda cacheado y se usa como
//! chunk size en las siguientes ejecuciones (AGENTS.md ss9).

use pyo3::prelude::*;

use crate::core::{Lease, ParamValue, SharedEngine};
use crate::errors::{is_reducible_size, to_py_err, CoreError};
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

/// Ejecuta un statement multi-row con un `(sql_builder, rows_chunk)` dado,
/// aplicando:
/// - **halve-and-retry** de chunk size contra SQL0101/SQL54001 (statement
///   demasiado grande: reducir el lote a la mitad y reintentar el mismo
///   rango), y
/// - **reintentos con backoff creciente** para errores transitorios
///   (SQL0913/SQL0904).
///
/// `build_sql(chunk_rows)` arma el SQL para un lote de `chunk_rows` filas.
/// `flatten(chunk)` aplana un chunk a `Vec<ParamValue>`.
///
/// Devuelve `(rows_affected, batches, max_rows_descubierto)`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_chunked_with_limits(
    lease: &Lease,
    chunk_size: usize,
    rows: &[Vec<ParamValue>],
    build_sql: impl Fn(usize) -> String,
    flatten: impl Fn(&[Vec<ParamValue>]) -> Vec<ParamValue>,
    max_rows: Option<usize>,
) -> Result<(i64, usize, Option<usize>), CoreError> {
    use crate::errors::{is_transient, TRANSIENT_BACKOFF_MS, TRANSIENT_MAX_RETRIES};

    // Tamano de lote a usar: el cacheado por halve-and-retry, o el pedido.
    let mut chunk_size = max_rows.unwrap_or(chunk_size.max(1)).max(1);
    let mut total = 0i64;
    let mut batches = 0usize;
    let mut discovered: Option<usize> = None;

    let mut idx = 0usize;
    while idx < rows.len() {
        // Divide el resto en lotes de `chunk_size`; si el lote que va a mandar
        // es demasiado grande (SQL0101/54001), baja el chunk_size y reintenta
        // este mismo rango (sin avanzar).
        let end = (idx + chunk_size).min(rows.len());
        let chunk = &rows[idx..end];
        let sql = build_sql(chunk.len());
        let flat = flatten(chunk);

        let affected = match lease.execute(&sql, &flat) {
            Ok(a) => a,
            Err(e) if is_reducible_size(e.native_code().unwrap_or(0)) => {
                if chunk_size <= 1 {
                    return Err(e);
                }
                chunk_size /= 2;
                discovered = Some(chunk_size);
                continue;
            }
            // Transitorio (SQL0913/SQL0904): reintentos con backoff creciente.
            Err(e) if is_transient(e.native_code().unwrap_or(0)) => {
                let mut last_err = e;
                let mut done = None;
                for attempt in 1..=TRANSIENT_MAX_RETRIES {
                    std::thread::sleep(std::time::Duration::from_millis(
                        TRANSIENT_BACKOFF_MS * u64::from(attempt),
                    ));
                    match lease.execute(&sql, &flat) {
                        Ok(a) => {
                            done = Some(a);
                            break;
                        }
                        Err(retry_e) if is_transient(retry_e.native_code().unwrap_or(0)) => {
                            last_err = retry_e; // sigue reintentando
                        }
                        Err(retry_e) => {
                            last_err = retry_e; // no-transitorio: aborta
                            break;
                        }
                    }
                }
                match done {
                    Some(a) => a,
                    None => return Err(last_err),
                }
            }
            Err(e) => return Err(e),
        };

        total += affected;
        batches += 1;
        idx = end;
    }

    Ok((total, batches, discovered))
}

/// Reescribe e inserta `rows` en sub-lotes de `chunk_size` filas por
/// statement, con halve-and-retry de chunk size. Usa el `Lease` recibido
/// (adquirido una vez por el caller -- A3). Sin exito parcial silencioso
/// (regla AGENTS.md ss4): un chunk que falla con error no-reducible aborta.
pub fn executebatch_core_with_lease(
    lease: &Lease,
    limits: &std::sync::Mutex<crate::core::StatementLimits>,
    sql: &str,
    rows: Vec<Vec<ParamValue>>,
    chunk_size: usize,
) -> Result<BulkReport, CoreError> {
    let (prefix, group) =
        split_single_row_insert(sql).map_err(|e| CoreError::Parameter(e.to_string()))?;

    let cached = limits
        .lock()
        .unwrap()
        .max_rows_per_statement
        .unwrap_or(chunk_size);

    let (rows_affected, batches, discovered) = execute_chunked_with_limits(
        lease,
        chunk_size,
        &rows,
        |n| format!("{prefix}{}", vec![group.as_str(); n].join(",")),
        |chunk| chunk.iter().flat_map(|r| r.iter().cloned()).collect(),
        Some(cached),
    )?;

    if let Some(d) = discovered {
        limits.lock().unwrap().max_rows_per_statement = Some(d);
    }

    Ok(BulkReport {
        rows_affected,
        batches,
    })
}

/// Convierte un iterable de filas a una lista de CHUNKS listos para worker
/// (`chunks[i]` = filas del chunk i). Se usa una vez; los workers toman los
/// chunks completos sin re-clonar filas.
pub fn rows_to_chunks(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    chunk_size: usize,
) -> PyResult<Vec<Vec<Vec<ParamValue>>>> {
    let chunk_size = chunk_size.max(1);
    let mut chunks: Vec<Vec<Vec<ParamValue>>> = Vec::new();
    let mut current: Vec<Vec<ParamValue>> = Vec::with_capacity(chunk_size);

    for row in rows.iter()? {
        let row = row?;
        let mut converted = Vec::new();
        for item in row.iter()? {
            converted.push(param_value_from_python(py, &item?)?);
        }
        current.push(converted);
        if current.len() == chunk_size {
            chunks.push(std::mem::take(&mut current));
        }
    }
    if !current.is_empty() {
        chunks.push(current);
    }
    Ok(chunks)
}

/// Ejecuta chunks ya convertidos en paralelo con `workers` leases. Cada
/// tarea toma sus chunks completos (sin clonar filas) y ejecuta cada uno con
/// su propio statement multi-row + halve-and-retry. Drena con
/// `collect::<Vec<Result>>()` -- se juntan TODOS los errores antes de
/// devolver el reporte (sin exito parcial silencioso).
pub async fn batch_execute_chunks_async(
    engine: &SharedEngine,
    sql: String,
    chunks: Vec<Vec<Vec<ParamValue>>>,
    workers: usize,
    fail_fast: bool,
) -> Result<ParallelReport, CoreError> {
    use futures::stream::{self, StreamExt};

    let workers = workers.max(1);
    // Repartir los chunks entre `workers` grupos disjuntos.
    let mut groups: Vec<Vec<Vec<Vec<ParamValue>>>> = vec![Vec::new(); workers];
    for (i, chunk) in chunks.into_iter().enumerate() {
        groups[i % workers].push(chunk);
    }

    let sql = std::sync::Arc::new(sql);
    let limits = engine.limits.clone();

    let results: Vec<Result<BulkReport, CoreError>> = stream::iter(groups)
        .map(|group| {
            let engine = engine.clone();
            let sql = sql.clone();
            let limits = limits.clone();
            async move {
                let lease = engine.acquire().await?;
                tokio::task::spawn_blocking(move || {
                    executebatch_group_with_lease(&lease, &limits, &sql, group)
                })
                .await
                .map_err(|e| CoreError::Connect(format!("panic: {e}")))?
            }
        })
        .buffer_unordered(workers)
        .collect()
        .await;

    let mut report = ParallelReport {
        rows_affected: 0,
        failures: Vec::new(),
    };
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(bulk) => report.rows_affected += bulk.rows_affected,
            Err(e) => {
                if fail_fast {
                    return Err(e);
                }
                report.failures.push(TaskFailure {
                    index: i,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(report)
}

/// Ejecuta los chunks de un grupo con UN lease, aplicando halve-and-retry a
/// cada chunk via `execute_chunked_with_limits`.
fn executebatch_group_with_lease(
    lease: &Lease,
    limits: &std::sync::Mutex<crate::core::StatementLimits>,
    sql: &str,
    group: Vec<Vec<Vec<ParamValue>>>,
) -> Result<BulkReport, CoreError> {
    let (prefix, placeholder) =
        split_single_row_insert(sql).map_err(|e| CoreError::Parameter(e.to_string()))?;

    let mut total = 0i64;
    let mut batches = 0usize;

    for chunk in group {
        // Cada chunk ya tiene `chunk_size` filas (o menos, el ultimo); se
        // ejecuta como UN statement multi-row con halve-and-retry.
        let n = chunk.len();
        let (affected, chunk_batches, discovered) = execute_chunked_with_limits(
            lease,
            n,
            &chunk,
            |rows_n| format!("{prefix}{}", vec![placeholder.as_str(); rows_n].join(",")),
            |flat_chunk| flat_chunk.iter().flatten().cloned().collect(),
            None,
        )?;
        total += affected;
        batches += chunk_batches;

        if let Some(d) = discovered {
            limits.lock().unwrap().max_rows_per_statement = Some(d);
        }
    }

    Ok(BulkReport {
        rows_affected: total,
        batches,
    })
}

/// Ejecuta `tasks` (lista de `(sql, rows)`) en paralelo, cada una con su
/// propio lease. Mismo patron de drenado que `batch_execute_chunks_async`.
pub async fn parallel_execute_async(
    engine: &SharedEngine,
    tasks: Vec<(String, Vec<Vec<ParamValue>>)>,
    chunk_size: usize,
    workers: usize,
    fail_fast: bool,
) -> Result<ParallelReport, CoreError> {
    use futures::stream::{self, StreamExt};

    let workers = workers.max(1);
    let limits = engine.limits.clone();

    let results: Vec<Result<BulkReport, CoreError>> = stream::iter(tasks.into_iter().enumerate())
        .map(|(_, (sql, task_rows))| {
            let engine = engine.clone();
            let limits = limits.clone();
            async move {
                let lease = engine.acquire().await?;
                tokio::task::spawn_blocking(move || {
                    executebatch_core_with_lease(&lease, &limits, &sql, task_rows, chunk_size)
                })
                .await
                .map_err(|e| CoreError::Connect(format!("panic: {e}")))?
            }
        })
        .buffer_unordered(workers)
        .collect()
        .await;

    let mut report = ParallelReport {
        rows_affected: 0,
        failures: Vec::new(),
    };
    for (i, res) in results.into_iter().enumerate() {
        match res {
            Ok(bulk) => report.rows_affected += bulk.rows_affected,
            Err(e) => {
                if fail_fast {
                    return Err(e);
                }
                report.failures.push(TaskFailure {
                    index: i,
                    error: e.to_string(),
                });
            }
        }
    }

    Ok(report)
}

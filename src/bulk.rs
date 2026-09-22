//! `executebatch`, `batch_execute`, `parallel_execute`: el motor de
//! escritura masiva.
//!
//! `executebatch` es el feature de performance insignia del proyecto -- el
//! driver IBM i Access no soporta `SQL_ATTR_PARAMSET_SIZE`, asi que arrays
//! de parametros no son una opcion (ver AGENTS.md ss1/ss4): en cambio,
//! `INSERT ... VALUES (?, ?, ...)` se reescribe a un `VALUES` multi-fila
//! (`VALUES (?,?),(?,?),...`) y se ejecuta en sub-lotes.
//!
//! Los limites de statement (SQL0101/SQL54001 en DB2 for i, HY090 del Driver
//! Manager/driver para statements inmensos, ver `errors::is_statement_too_large`)
//! se descubren con **halve-and-retry**: la pieza que excede se parte al medio
//! y se reintenta. El presupuesto se memoiza por engine en unidades UTF-16 del
//! statement generado (`core::StatementLimits`), no en filas -- asi una tabla
//! angosta no queda castigada por el limite de una ancha.
//!
//! Para paralelizar, la misma pieza partida se reparte entre N workers con
//! **una cola de trabajo compartida**: cada pieza se ejecuta UNA sola vez (no
//! hay doble INSERT) y el primer worker que descubre el limite achica la pieza
//! para todos. Cada worker usa su propio `Lease` (su propia conexion del pool).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::prelude::*;

use crate::core::{Lease, ParamValue, SharedEngine, StatementLimits};
use crate::errors::{
    is_statement_too_large, is_transient, to_py_err, CoreError, TRANSIENT_BACKOFF_MS,
    TRANSIENT_MAX_RETRIES,
};
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

// ---------------------------------------------------------------------------
// Cola de trabajo compartida (halve-and-retry + workers)
// ---------------------------------------------------------------------------

/// Rango de filas `[start, start+len)` a insertar con UN statement multi-fila.
#[derive(Debug, Clone, Copy)]
struct Piece {
    start: usize,
    len: usize,
}

/// Cola de piezas compartida por los workers. Cada pieza se toma UNA vez; un
/// fallo por tamano la parte al medio y re-encola las dos mitades. Nunca hay
/// doble ejecucion de un mismo rango (el INSERT no se duplica).
struct WorkQueue {
    pieces: Mutex<VecDeque<Piece>>,
}

impl WorkQueue {
    fn new(total: usize, initial: usize) -> Self {
        let mut pieces = VecDeque::new();
        if total > 0 {
            let initial = initial.max(1);
            let mut start = 0;
            while start < total {
                let len = initial.min(total - start);
                pieces.push_back(Piece { start, len });
                start += len;
            }
        }
        WorkQueue {
            pieces: Mutex::new(pieces),
        }
    }

    fn claim(&self) -> Option<Piece> {
        self.pieces.lock().unwrap().pop_front()
    }

    /// Solo se llama con `p.len > 1`: las dos mitades tienen >= 1 fila.
    fn requeue_halves(&self, p: Piece) {
        let half = (p.len / 2).max(1);
        let second = Piece {
            start: p.start + half,
            len: p.len - half,
        };
        let first = Piece {
            start: p.start,
            len: half,
        };
        let mut q = self.pieces.lock().unwrap();
        // push_front en orden inverso para que la primera mitad salga primero.
        q.push_front(second);
        q.push_front(first);
    }
}

/// Resultado parcial de un worker.
#[derive(Default)]
struct WorkerOutcome {
    rows_affected: i64,
    batches: usize,
    failures: Vec<TaskFailure>,
}

/// Resultado interno del motor de workers (Rust-only: los `#[pyclass]` no
/// llevan `batches`, y `MergeReport` si).
pub(crate) struct WorkerReport {
    pub rows_affected: i64,
    pub batches: usize,
    pub failures: Vec<TaskFailure>,
}

/// `(unidades UTF-16 de un statement de 1 fila, unidades por fila adicional)`.
/// Se mide con el propio `build_sql`, asi sirve igual para el `INSERT`
/// reescrito que para el `MERGE`.
fn shape_units<F: Fn(usize) -> String + ?Sized>(build_sql: &F) -> (usize, usize) {
    let one = build_sql(1).encode_utf16().count();
    let two = build_sql(2).encode_utf16().count();
    (one, two.saturating_sub(one))
}

/// Filas por pieza inicial: el menor entre lo pedido y lo que entra en el
/// presupuesto actual de unidades. Sin presupuesto observado, usa lo pedido.
fn initial_piece_size<F: Fn(usize) -> String + ?Sized>(
    build_sql: &F,
    limits: &Mutex<StatementLimits>,
    requested: usize,
    total: usize,
) -> usize {
    let requested = requested.max(1);
    let (one, row) = shape_units(build_sql);
    let budget = limits.lock().unwrap().budget_units();
    let by_budget = match budget {
        Some(b) if row > 0 => ((b.saturating_sub(one)) / row).saturating_add(1),
        _ => requested,
    };
    by_budget.min(requested).clamp(1, total.max(1))
}

/// `lease.execute` con reintentos y backoff creciente para errores
/// transitorios (SQL0913/SQL0904).
fn exec_with_retry(lease: &Lease, sql: &str, flat: &[ParamValue]) -> Result<i64, CoreError> {
    match lease.execute(sql, flat) {
        Ok(a) => Ok(a),
        Err(e) if is_transient(e.native_code().unwrap_or(0)) => {
            let mut last_err = e;
            for attempt in 1..=TRANSIENT_MAX_RETRIES {
                std::thread::sleep(std::time::Duration::from_millis(
                    TRANSIENT_BACKOFF_MS * u64::from(attempt),
                ));
                match lease.execute(sql, flat) {
                    Ok(a) => return Ok(a),
                    Err(retry) if is_transient(retry.native_code().unwrap_or(0)) => {
                        last_err = retry;
                    }
                    Err(retry) => return Err(retry),
                }
            }
            Err(last_err)
        }
        Err(e) => Err(e),
    }
}

/// Un worker drena la cola hasta vaciarla (o hasta que `abort` se dispare).
///
/// `stop_on_error = true` (camino secuencial de `executebatch`/`merge`, y
/// `fail_fast=true`): el primer error no reducible aborta y se propaga.
/// `stop_on_error = false`: se registra en `outcome.failures` y se sigue (sin
/// exito parcial silencioso: el caller devuelve todos los fallos).
fn drain<F: Fn(usize) -> String + ?Sized>(
    lease: &Lease,
    build_sql: &F,
    rows: &[Vec<ParamValue>],
    limits: &Mutex<StatementLimits>,
    queue: &WorkQueue,
    abort: &AtomicBool,
    stop_on_error: bool,
) -> Result<WorkerOutcome, CoreError> {
    let mut outcome = WorkerOutcome::default();
    while !abort.load(Ordering::Relaxed) {
        let Some(piece) = queue.claim() else {
            break;
        };
        let sql = build_sql(piece.len);
        let units = sql.encode_utf16().count();
        let flat: Vec<ParamValue> = rows[piece.start..piece.start + piece.len]
            .iter()
            .flat_map(|r| r.iter().cloned())
            .collect();

        match exec_with_retry(lease, &sql, &flat) {
            Ok(affected) => {
                outcome.rows_affected += affected;
                outcome.batches += 1;
                limits.lock().unwrap().record_ok(units);
            }
            Err(e) if is_statement_too_large(&e) && piece.len > 1 => {
                limits.lock().unwrap().record_failed(units);
                queue.requeue_halves(piece);
            }
            Err(e) => {
                if stop_on_error {
                    abort.store(true, Ordering::Relaxed);
                    return Err(e);
                }
                outcome.failures.push(TaskFailure {
                    index: piece.start,
                    error: e.to_string(),
                });
            }
        }
    }
    Ok(outcome)
}

/// Ejecuta `rows` en sub-lotes con UN `Lease` (secuencial), con piezas de a
/// `requested_piece` acotadas por el presupuesto de statement. Un error no
/// reducible aborta (regla AGENTS.md ss4: sin exito parcial silencioso).
pub(crate) fn execute_chunked<F: Fn(usize) -> String + ?Sized>(
    lease: &Lease,
    limits: &Mutex<StatementLimits>,
    build_sql: &F,
    rows: &[Vec<ParamValue>],
    requested_piece: usize,
) -> Result<(i64, usize), CoreError> {
    let initial = initial_piece_size(build_sql, limits, requested_piece, rows.len());
    let queue = WorkQueue::new(rows.len(), initial);
    let abort = AtomicBool::new(false);
    let outcome = drain(lease, build_sql, rows, limits, &queue, &abort, true)?;
    Ok((outcome.rows_affected, outcome.batches))
}

/// Reescribe e inserta `rows` en sub-lotes de `chunk_size` filas por
/// statement, con halve-and-retry de tamano. Usa el `Lease` recibido
/// (adquirido una vez por el caller -- A3).
pub fn executebatch_core_with_lease(
    lease: &Lease,
    limits: &Mutex<StatementLimits>,
    sql: &str,
    rows: Vec<Vec<ParamValue>>,
    chunk_size: usize,
) -> Result<BulkReport, CoreError> {
    let (prefix, group) =
        split_single_row_insert(sql).map_err(|e| CoreError::Parameter(e.to_string()))?;
    let build = |n: usize| format!("{prefix}{}", vec![group.as_str(); n].join(","));
    let (rows_affected, batches) = execute_chunked(lease, limits, &build, &rows, chunk_size)?;
    Ok(BulkReport {
        rows_affected,
        batches,
    })
}

/// Motor de workers: N leases (uno por worker) drenan la misma cola de piezas.
/// Devuelve el reporte interno con `batches` (para `merge`) ademas del conteo.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn execute_workers_async(
    engine: &SharedEngine,
    build_sql: Arc<dyn Fn(usize) -> String + Send + Sync>,
    rows: Arc<Vec<Vec<ParamValue>>>,
    workers: usize,
    requested_piece: usize,
    stop_on_error: bool,
) -> Result<WorkerReport, CoreError> {
    use futures::stream::{self, StreamExt};

    let total = rows.len();
    if total == 0 {
        return Ok(WorkerReport {
            rows_affected: 0,
            batches: 0,
            failures: Vec::new(),
        });
    }

    let limits = engine.limits.clone();
    let initial = initial_piece_size(&*build_sql, &limits, requested_piece, total);
    let queue = Arc::new(WorkQueue::new(total, initial));
    let abort = Arc::new(AtomicBool::new(false));
    // No mas workers que filas, ni que conexiones del pool (los workers de mas
    // solo esperarian en `acquire` y podrian terminar en PoolTimeout).
    let workers = workers.max(1).min(total).min(engine.pool_size.max(1));

    let results: Vec<Result<WorkerOutcome, CoreError>> = stream::iter(0..workers)
        .map(|_| {
            let engine = engine.clone();
            let build_sql = build_sql.clone();
            let rows = rows.clone();
            let limits = limits.clone();
            let queue = queue.clone();
            let abort = abort.clone();
            async move {
                let lease = engine.acquire().await?;
                tokio::task::spawn_blocking(move || {
                    drain(
                        &lease,
                        &*build_sql,
                        rows.as_slice(),
                        &limits,
                        &queue,
                        &abort,
                        stop_on_error,
                    )
                })
                .await
                .map_err(|e| CoreError::Connect(format!("panic: {e}")))?
            }
        })
        .buffer_unordered(workers)
        .collect()
        .await;

    let mut report = WorkerReport {
        rows_affected: 0,
        batches: 0,
        failures: Vec::new(),
    };
    for res in results {
        match res {
            Ok(outcome) => {
                report.rows_affected += outcome.rows_affected;
                report.batches += outcome.batches;
                report.failures.extend(outcome.failures);
            }
            // `stop_on_error` (fail_fast): el primer error real se propaga.
            Err(e) => return Err(e),
        }
    }
    Ok(report)
}

/// `executebatch` en paralelo: mismo contrato (rowcount + batches) pero
/// reparte las piezas entre `workers` conexiones. `fail_fast = true` (el
/// unico modo con sentido para un batch homogeneo).
pub(crate) async fn executebatch_parallel_async(
    engine: &SharedEngine,
    sql: String,
    rows: Vec<Vec<ParamValue>>,
    chunk_size: usize,
    workers: usize,
) -> Result<BulkReport, CoreError> {
    let (prefix, group) =
        split_single_row_insert(&sql).map_err(|e| CoreError::Parameter(e.to_string()))?;
    let build: Arc<dyn Fn(usize) -> String + Send + Sync> =
        Arc::new(move |n| format!("{prefix}{}", vec![group.as_str(); n].join(",")));
    let report =
        execute_workers_async(engine, build, Arc::new(rows), workers, chunk_size, true).await?;
    Ok(BulkReport {
        rows_affected: report.rows_affected,
        batches: report.batches,
    })
}

/// `batch_execute`: reescribe el INSERT y reparte en paralelo con `workers`.
pub(crate) async fn batch_execute_async(
    engine: &SharedEngine,
    sql: String,
    rows: Vec<Vec<ParamValue>>,
    chunk_size: usize,
    workers: usize,
    fail_fast: bool,
) -> Result<ParallelReport, CoreError> {
    let (prefix, group) =
        split_single_row_insert(&sql).map_err(|e| CoreError::Parameter(e.to_string()))?;
    let build: Arc<dyn Fn(usize) -> String + Send + Sync> =
        Arc::new(move |n| format!("{prefix}{}", vec![group.as_str(); n].join(",")));
    let report = execute_workers_async(
        engine,
        build,
        Arc::new(rows),
        workers,
        chunk_size,
        fail_fast,
    )
    .await?;
    Ok(ParallelReport {
        rows_affected: report.rows_affected,
        failures: report.failures,
    })
}

/// Ejecuta `tasks` (lista de `(sql, rows)`) en paralelo, cada una con su
/// propio lease (secuencial dentro de cada tarea). Mismo drenado sin exito
/// parcial silencioso.
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

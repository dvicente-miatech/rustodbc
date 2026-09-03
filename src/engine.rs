//! `Db2iEngine` / `ConnectionLease` / `Transaction`: los `#[pyclass]`
//! publicos que envuelven `core::{Engine, Lease}`.
//!
//! Toda llamada que habla ODBC corre dentro de `tokio::task::spawn_blocking`
//! -- el GIL se libera ANTES de entrar (los datos que cruzan al hilo
//! bloqueante ya son valores Rust puros, extraidos de los argumentos Python
//! mientras se tenia el GIL) y se re-adquiere recien al construir el
//! resultado Python del otro lado. Regla dura de AGENTS.md ss4: el GIL nunca
//! se sostiene en el hilo bloqueante.
//!
//! Los cuerpos async de cada operacion estan en las funciones libres
//! `*_impl` de abajo, reutilizadas por `Db2iEngine` (via `future_into_py`)
//! y por `crate::blocking::BlockingEngine` (via `runtime.block_on`) -- una
//! sola implementacion, dos frontends (async/sync).

use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyList;
use secrecy::SecretString;

use crate::config::{Credentials, EngineOptions};
use crate::core::{self, ColumnValue, ParamValue, SharedEngine};
use crate::errors::to_py_err;
use crate::params::params_from_python;
use crate::rows::{batch_to_pylist, column_value_to_py};
use crate::stream::BatchStream;

pub(crate) fn resolve_driver(explicit: Option<&str>) -> PyResult<String> {
    if let Some(d) = explicit {
        return Ok(d.to_string());
    }
    let env = core::ffi::environment().map_err(to_py_err)?;
    let installed = env.list_drivers();
    for pref in crate::config::PREFERRED_DRIVERS {
        if installed.iter().any(|d| d.eq_ignore_ascii_case(pref)) {
            return Ok((*pref).to_string());
        }
    }
    Ok(crate::config::DEFAULT_DRIVER_FALLBACK.to_string())
}

pub(crate) fn resolve_dsn(credentials: &Credentials) -> PyResult<SecretString> {
    if credentials.raw_dsn.is_some() {
        // build_dsn ignora el driver resuelto cuando hay raw_dsn -- no hace
        // falta ni vale la pena probar drivers instalados en este camino.
        return Ok(credentials.build_dsn(""));
    }
    let driver = resolve_driver(credentials.driver.as_deref())?;
    Ok(credentials.build_dsn(&driver))
}

// ---------------------------------------------------------------------------
// Cuerpos async compartidos (async frontend -> future_into_py; blocking ->
// runtime.block_on)
// ---------------------------------------------------------------------------

/// Crea un `Engine` (pool) de forma async, dentro de `spawn_blocking`.
pub(crate) async fn connect_impl(
    dsn: SecretString,
    options: EngineOptions,
) -> PyResult<core::Engine> {
    let pool_size = options.pool_size;
    let login_timeout = options.login_timeout;
    tokio::task::spawn_blocking(move || core::Engine::connect(dsn, pool_size, login_timeout))
        .await
        .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
        .map_err(to_py_err)
}

async fn run_blocking<T, F>(engine: &SharedEngine, f: F) -> PyResult<T>
where
    T: Send + 'static,
    F: FnOnce(&core::Lease) -> Result<T, crate::errors::CoreError> + Send + 'static,
{
    // Adquirir el lease es async (puede implicar crear una conexion nueva,
    // que a su vez hace su propio spawn_blocking dentro de
    // `ConnManager::create`) -- se espera aca, en la tarea async normal,
    // NUNCA dentro de un `spawn_blocking` (evita bloquear un hilo del pool
    // de blocking esperando a otro spawn_blocking anidado).
    let lease = engine.acquire().await.map_err(to_py_err)?;

    // El trabajo ODBC propiamente dicho (la llamada bloqueante real) si
    // corre en `spawn_blocking`, con el GIL ya liberado del lado de Python
    // desde que `future_into_py` entrego el control a este future.
    tokio::task::spawn_blocking(move || f(&lease).map_err(to_py_err))
        .await
        .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
}

pub(crate) async fn execute_impl(
    engine: SharedEngine,
    sql: String,
    params: Vec<ParamValue>,
) -> PyResult<i64> {
    run_blocking(&engine, move |lease| lease.execute(&sql, &params)).await
}

pub(crate) async fn fetch_all_impl(
    engine: SharedEngine,
    options: EngineOptions,
    sql: String,
    params: Vec<ParamValue>,
) -> PyResult<Py<PyList>> {
    let (columns, rows) = run_blocking(&engine, move |lease| lease.query(&sql, &params)).await?;
    Python::with_gil(|py| {
        batch_to_pylist(
            py,
            &columns,
            &rows,
            options.strip_char_padding,
            &options.decimal_mode,
        )
    })
}

pub(crate) async fn fetch_one_impl(
    engine: SharedEngine,
    options: EngineOptions,
    sql: String,
    params: Vec<ParamValue>,
) -> PyResult<PyObject> {
    let (columns, rows) = run_blocking(&engine, move |lease| lease.query(&sql, &params)).await?;
    Python::with_gil(|py| -> PyResult<PyObject> {
        if rows.is_empty() {
            return Ok(py.None());
        }
        let list = batch_to_pylist(
            py,
            &columns,
            &rows[..1],
            options.strip_char_padding,
            &options.decimal_mode,
        )?;
        let list_bound = list.bind(py);
        Ok(list_bound.get_item(0)?.unbind())
    })
}

pub(crate) async fn fetch_value_impl(
    engine: SharedEngine,
    options: EngineOptions,
    sql: String,
    params: Vec<ParamValue>,
) -> PyResult<PyObject> {
    let (columns, rows) = run_blocking(&engine, move |lease| lease.query(&sql, &params)).await?;
    Python::with_gil(|py| -> PyResult<PyObject> {
        let (Some(row), Some(meta)) = (rows.first(), columns.first()) else {
            return Ok(py.None());
        };
        let value = row.first().unwrap_or(&ColumnValue::Null);
        column_value_to_py(
            py,
            meta,
            value,
            options.strip_char_padding,
            &options.decimal_mode,
        )
    })
}

pub(crate) async fn fetch_column_impl(
    engine: SharedEngine,
    options: EngineOptions,
    sql: String,
    params: Vec<ParamValue>,
) -> PyResult<Py<PyList>> {
    let (columns, rows) = run_blocking(&engine, move |lease| lease.query(&sql, &params)).await?;
    Python::with_gil(|py| -> PyResult<Py<PyList>> {
        let out = PyList::empty_bound(py);
        if let Some(meta) = columns.first() {
            for row in &rows {
                let value = row.first().unwrap_or(&ColumnValue::Null);
                let py_value = column_value_to_py(
                    py,
                    meta,
                    value,
                    options.strip_char_padding,
                    &options.decimal_mode,
                )?;
                out.append(py_value)?;
            }
        }
        Ok(out.unbind())
    })
}

/// Ejecuta una consulta en modo streaming: adquiere un lease y devuelve un
/// `(Lease, RowCursor)` listo para `BatchStream`/`BlockingBatchStream`.
pub(crate) async fn query_cursor_impl(
    engine: SharedEngine,
    sql: String,
    params: Vec<ParamValue>,
) -> PyResult<(core::Lease, core::RowCursor)> {
    let lease = engine.acquire().await.map_err(to_py_err)?;
    tokio::task::spawn_blocking(move || match lease.query_cursor(&sql, &params) {
        Ok(cursor) => Ok((lease, cursor)),
        Err(e) => Err(e),
    })
    .await
    .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
    .map_err(to_py_err)
}

pub(crate) async fn executebatch_impl(
    engine: SharedEngine,
    sql: String,
    rows: Vec<Vec<ParamValue>>,
    chunk_size: usize,
) -> PyResult<crate::bulk::BulkReport> {
    // A3: un solo lease para todo el batch (no uno por chunk).
    let lease = engine.acquire().await.map_err(to_py_err)?;
    let limits = engine.limits.clone();
    tokio::task::spawn_blocking(move || {
        crate::bulk::executebatch_core_with_lease(&lease, &limits, &sql, rows, chunk_size)
    })
    .await
    .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
    .map_err(to_py_err)
}

pub(crate) async fn batch_execute_impl(
    engine: SharedEngine,
    sql: String,
    chunks: Vec<Vec<Vec<ParamValue>>>,
    workers: usize,
    fail_fast: bool,
) -> PyResult<crate::bulk::ParallelReport> {
    crate::bulk::batch_execute_chunks_async(&engine, sql, chunks, workers, fail_fast)
        .await
        .map_err(to_py_err)
}

pub(crate) async fn parallel_execute_impl(
    engine: SharedEngine,
    tasks: Vec<(String, Vec<Vec<ParamValue>>)>,
    chunk_size: usize,
    workers: usize,
    fail_fast: bool,
) -> PyResult<crate::bulk::ParallelReport> {
    crate::bulk::parallel_execute_async(&engine, tasks, chunk_size, workers, fail_fast)
        .await
        .map_err(to_py_err)
}

pub(crate) async fn call_proc_impl(
    engine: SharedEngine,
    schema: String,
    proc: String,
    params: Py<PyAny>,
    strip_char_padding: bool,
    decimal_mode: String,
) -> PyResult<Py<crate::proc::ProcResult>> {
    tokio::task::spawn_blocking(move || {
        Python::with_gil(|py| {
            let bound = params.bind(py);
            crate::proc::call_proc_sync(
                &engine,
                &schema,
                &proc,
                bound,
                strip_char_padding,
                &decimal_mode,
            )
        })
    })
    .await
    .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
}

/// Variante posicional (`call_proc_args`): misma vida que `call_proc_impl` pero
/// despacha a `proc::call_proc_args_sync` (validacion por tipo/orden).
pub(crate) async fn call_proc_args_impl(
    engine: SharedEngine,
    schema: String,
    proc: String,
    params: Py<PyAny>,
    strip_char_padding: bool,
    decimal_mode: String,
) -> PyResult<Py<crate::proc::ProcResult>> {
    tokio::task::spawn_blocking(move || {
        Python::with_gil(|py| {
            let bound = params.bind(py);
            crate::proc::call_proc_args_sync(
                &engine,
                &schema,
                &proc,
                bound,
                strip_char_padding,
                &decimal_mode,
            )
        })
    })
    .await
    .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?
}

#[pyclass(module = "rustodbc")]
pub struct Db2iEngine {
    pub(crate) engine: SharedEngine,
    pub(crate) options: EngineOptions,
}

impl Db2iEngine {
    fn connect_with_dsn(
        py: Python<'_>,
        dsn: SecretString,
        options: EngineOptions,
    ) -> PyResult<Bound<'_, PyAny>> {
        let opts_for_future = options.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let engine = connect_impl(dsn, opts_for_future.clone()).await?;

            Python::with_gil(|py| {
                Py::new(
                    py,
                    Db2iEngine {
                        engine: Arc::new(engine),
                        options: opts_for_future,
                    },
                )
            })
        })
    }
}

#[pymethods]
impl Db2iEngine {
    #[staticmethod]
    #[pyo3(signature = (credentials, options=None))]
    fn connect<'py>(
        py: Python<'py>,
        credentials: PyRef<'_, Credentials>,
        options: Option<EngineOptions>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // `credentials` es un `PyRef`; la deref coercion de Rust lo convierte
        // a `&Credentials` automaticamente en el argumento.
        let dsn = resolve_dsn(&credentials)?;
        Db2iEngine::connect_with_dsn(py, dsn, options.unwrap_or_default())
    }

    #[staticmethod]
    #[pyo3(signature = (client_code, environment=None, options=None))]
    fn from_env<'py>(
        py: Python<'py>,
        client_code: String,
        environment: Option<String>,
        options: Option<EngineOptions>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let credentials = Credentials::from_env(&client_code, environment.as_deref())?;
        let dsn = resolve_dsn(&credentials)?;
        Db2iEngine::connect_with_dsn(py, dsn, options.unwrap_or_default())
    }

    /// Cierra el pool. Idempotente -- llamar dos veces no rompe nada.
    fn close(&self) {
        self.engine.close();
    }

    fn __aenter__<'py>(slf: PyRef<'py, Self>, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let slf_py: Py<Self> = slf.into();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(slf_py) })
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.close();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(false) })
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = convert_params(py, params)?;
        let engine = self.engine.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, execute_impl(engine, sql, params))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_all<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = convert_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, fetch_all_impl(engine, options, sql, params))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_one<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = convert_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        pyo3_async_runtimes::tokio::future_into_py(py, fetch_one_impl(engine, options, sql, params))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_value<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = convert_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            fetch_value_impl(engine, options, sql, params),
        )
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_column<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = convert_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            fetch_column_impl(engine, options, sql, params),
        )
    }

    /// Devuelve un `BatchStream` que ejecuta `sql` y va trayendo lotes de
    /// `batch_size` filas (default `EngineOptions.stream_batch_size`) sin
    /// materializar el result set completo en memoria.
    #[pyo3(signature = (sql, params=None, batch_size=None))]
    fn stream_batches<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params = convert_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        let batch_size = batch_size.unwrap_or(options.stream_batch_size);
        let prefetch = options.prefetch_batches;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            let (lease, cursor) = query_cursor_impl(engine, sql, params).await?;
            Python::with_gil(|py| {
                Py::new(
                    py,
                    BatchStream::new(lease, cursor, batch_size, prefetch, options),
                )
            })
        })
    }

    #[pyo3(signature = (sql, params=None, batch_size=None))]
    fn stream<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        params: Option<Bound<'py, PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Bound<'py, PyAny>> {
        // `stream()` es lo mismo que `stream_batches()` -- ambos iteran por
        // lotes de filas (`BatchStream`); ver `.pyi`.
        self.stream_batches(py, sql, params, batch_size)
    }

    /// Reescribe `INSERT ... VALUES (?,...)` a un `VALUES` multi-fila e
    /// inserta `rows` en sub-lotes de `EngineOptions.batch_size` filas por
    /// statement. Ver `bulk::executebatch_core` para el detalle y las
    /// simplificaciones deliberadas de esta primera pasada.
    #[pyo3(signature = (sql, rows))]
    fn executebatch<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        rows: Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let rows = crate::bulk::rows_to_param_values(py, &rows)?;
        let engine = self.engine.clone();
        let chunk_size = self.options.batch_size;
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            executebatch_impl(engine, sql, rows, chunk_size),
        )
    }

    /// Ejecuta `INSERT ... VALUES (?,...)` contra `rows` en paralelo
    /// (`workers` conexiones del pool). Reglas: sin exito parcial silencioso
    /// (se juntan todos los errores), `fail_fast=True` cancela el resto al
    /// primer error.
    #[pyo3(signature = (sql, rows, *, max_workers=None, fail_fast=false))]
    fn batch_execute<'py>(
        &self,
        py: Python<'py>,
        sql: String,
        rows: Bound<'py, PyAny>,
        max_workers: Option<usize>,
        fail_fast: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let chunk_size = self.options.batch_size;
        // 2d: convertir a chunks una sola vez (sin clonar filas por worker).
        let chunks = crate::bulk::rows_to_chunks(py, &rows, chunk_size)?;
        let engine = self.engine.clone();
        let workers = max_workers.unwrap_or(self.options.max_workers);
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            batch_execute_impl(engine, sql, chunks, workers, fail_fast),
        )
    }

    /// Ejecuta `tasks` (lista de `(sql, rows)`) en paralelo, cada una con su
    /// propio lease. Mismo drenado sin exito parcial silencioso.
    #[pyo3(signature = (tasks, *, max_workers=None, fail_fast=false))]
    fn parallel_execute<'py>(
        &self,
        py: Python<'py>,
        tasks: Bound<'py, PyAny>,
        max_workers: Option<usize>,
        fail_fast: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let engine = self.engine.clone();
        let chunk_size = self.options.batch_size;
        let workers = max_workers.unwrap_or(self.options.max_workers);

        let mut tasks_vec = Vec::new();
        for item in tasks.iter()? {
            let item = item?;
            let tuple = item.downcast::<pyo3::types::PyTuple>().map_err(|_| {
                to_py_err(crate::errors::CoreError::Parameter(
                    "parallel_execute: cada tarea debe ser una tupla (sql, rows)".to_string(),
                ))
            })?;
            if tuple.len() != 2 {
                return Err(to_py_err(crate::errors::CoreError::Parameter(
                    "parallel_execute: cada tarea debe ser una tupla (sql, rows)".to_string(),
                )));
            }
            let sql: String = tuple.get_item(0)?.extract()?;
            let rows = crate::bulk::rows_to_param_values(py, &tuple.get_item(1)?)?;
            tasks_vec.push((sql, rows));
        }

        pyo3_async_runtimes::tokio::future_into_py(
            py,
            parallel_execute_impl(engine, tasks_vec, chunk_size, workers, fail_fast),
        )
    }

    /// `CALL schema.proc({nombre: valor})` -- ver `proc.rs` para el detalle
    /// (dict por nombre + OUT/INOUT devueltos en `ProcResult.out_params`).
    #[pyo3(signature = (schema, proc, params=None))]
    fn call_proc<'py>(
        &self,
        py: Python<'py>,
        schema: String,
        proc: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params_owned: Py<PyAny> = match params {
            Some(p) => p.unbind(),
            None => py.None(),
        };
        let engine = self.engine.clone();
        let strip = self.options.strip_char_padding;
        let decimal_mode = self.options.decimal_mode.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            call_proc_impl(engine, schema, proc, params_owned, strip, decimal_mode),
        )
    }

    /// `CALL schema.proc(val1, val2, ...)` -- variante POSICIONAL del
    /// `call_proc`, sin nombres. `params` es una secuencia (`list`/`tuple`) en
    /// el mismo orden ordinal que el catalogo (`SQLProcedureColumns`); los OUT
    /// van como `None`. Valida cada valor contra el tipo/largo declarado del
    /// parametro (largo en tipos de caracter, parseabilidad numerica, bit,
    /// fecha) y, si hay fallos, levanta `ProcValidationError` con el mensaje
    /// agregado. Los errores internos del procedimiento salen como
    /// `QueryError`. Ver `proc.rs`.
    #[pyo3(signature = (schema, proc, params=None))]
    fn call_proc_args<'py>(
        &self,
        py: Python<'py>,
        schema: String,
        proc: String,
        params: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let params_owned: Py<PyAny> = match params {
            Some(p) => p.unbind(),
            None => py.None(),
        };
        let engine = self.engine.clone();
        let strip = self.options.strip_char_padding;
        let decimal_mode = self.options.decimal_mode.clone();
        pyo3_async_runtimes::tokio::future_into_py(
            py,
            call_proc_args_impl(engine, schema, proc, params_owned, strip, decimal_mode),
        )
    }

    /// Devuelve un `TableSync` que hace MERGE/upsert contra este engine
    /// (`dest`), opcionalmente leyendo de `source` (por ahora `source` no se
    /// usa en `merge()` -- queda para un `transfer()` futuro que lea de un
    /// engine y escriba en otro en un solo paso). Composicion, nunca
    /// herencia (ver `tablesync/mod.rs`).
    #[cfg(feature = "tablesync")]
    #[pyo3(signature = (source=None))]
    fn table_sync<'py>(
        &self,
        py: Python<'py>,
        source: Option<Py<Db2iEngine>>,
    ) -> crate::tablesync::TableSync {
        crate::tablesync::TableSync::new(
            self.engine.clone(),
            source.map(|s| s.borrow(py).engine.clone()),
            self.options.merge_chunk_size,
            None,
        )
    }
}

fn convert_params(py: Python<'_>, params: Option<Bound<'_, PyAny>>) -> PyResult<Vec<ParamValue>> {
    match params {
        None => Ok(Vec::new()),
        Some(p) => {
            let _ = py;
            params_from_python(&p)
        }
    }
}

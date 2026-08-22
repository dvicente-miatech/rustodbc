//! `BlockingEngine`: fachada sincrona en Rust (no un wrapper de
//! `asyncio.run`). Proyeccion delgada sobre la API async ya estable: reusa
//! los `*_impl` de `engine.rs` ejecutandolos con un runtime tokio propio via
//! `block_on` con el GIL liberado (`Python::allow_threads`).
//!
//! Pensada para los ~50 call-sites del consumidor que hoy envuelven
//! `ISeriesConnection` en `asyncio.to_thread(...)` desde codigo sincrono
//! (crons de arq, parsers).
//!
//! Guard: levanta `InterfaceError` si se llama desde un hilo con un event
//! loop de asyncio *corriendo* -- para que nadie termine bloqueando el loop
//! de arq. Si necesitas async, usa `Db2iEngine`.

use std::sync::Arc;

use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::types::PyList;
use secrecy::SecretString;

use crate::config::{Credentials, EngineOptions};
use crate::core::{Lease, ParamValue, SharedEngine};
use crate::engine::{
    call_proc_impl, connect_impl, execute_impl, executebatch_impl, fetch_all_impl,
    fetch_column_impl, fetch_one_impl, fetch_value_impl, query_cursor_impl, resolve_dsn,
};
use crate::errors::{to_py_err, CoreError};
use crate::params::params_from_python;
use crate::rows::batch_to_pylist;

fn check_no_running_loop(py: Python<'_>) -> PyResult<()> {
    let asyncio = py.import_bound("asyncio")?;
    let get_running = asyncio.getattr("get_running_loop")?;
    match get_running.call0() {
        Ok(_) => Err(crate::errors::InterfaceError::new_err(
            "BlockingEngine: hay un event loop de asyncio corriendo en este hilo. Usa la API \
             async (Db2iEngine) o llama desde un hilo sin event loop.",
        )),
        Err(_) => Ok(()),
    }
}

fn to_params(py: Python<'_>, params: Option<Bound<'_, PyAny>>) -> PyResult<Vec<ParamValue>> {
    match params {
        None => Ok(Vec::new()),
        Some(p) => {
            let _ = py;
            params_from_python(&p)
        }
    }
}

#[pyclass(module = "rustodbc")]
pub struct BlockingEngine {
    engine: SharedEngine,
    options: EngineOptions,
    runtime: tokio::runtime::Runtime,
}

impl BlockingEngine {
    fn connect_with_dsn(dsn: SecretString, options: EngineOptions) -> PyResult<Self> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|e| {
                to_py_err(CoreError::Configuration(format!(
                    "no se pudo armar el runtime tokio: {e}"
                )))
            })?;

        let opts_for_connect = options.clone();
        let engine = runtime.block_on(async move { connect_impl(dsn, &opts_for_connect).await })?;

        Ok(BlockingEngine {
            engine: Arc::new(engine),
            options,
            runtime,
        })
    }

    /// Ejecuta `f` (una tarea async de `engine.rs`) en el runtime propio,
    /// con el GIL liberado durante todo el bloqueo.
    fn block_on<T>(
        &self,
        fut: impl std::future::Future<Output = PyResult<T>> + Send,
    ) -> PyResult<T> {
        self.runtime.block_on(fut)
    }
}

#[pymethods]
impl BlockingEngine {
    #[staticmethod]
    #[pyo3(signature = (credentials, options=None))]
    fn connect(credentials: &Credentials, options: Option<EngineOptions>) -> PyResult<Self> {
        let dsn = resolve_dsn(credentials)?;
        BlockingEngine::connect_with_dsn(dsn, options.unwrap_or_default())
    }

    #[staticmethod]
    #[pyo3(signature = (client_code, environment=None, options=None))]
    fn from_env(
        py: Python<'_>,
        client_code: String,
        environment: Option<String>,
        options: Option<EngineOptions>,
    ) -> PyResult<Self> {
        check_no_running_loop(py)?;
        let credentials = Credentials::from_env(&client_code, environment.as_deref())?;
        let dsn = resolve_dsn(&credentials)?;
        BlockingEngine::connect_with_dsn(dsn, options.unwrap_or_default())
    }

    /// Cierra el pool. Idempotente.
    fn close(&self) {
        self.engine.close();
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        check_no_running_loop(py)?;
        let params = to_params(py, params)?;
        let engine = self.engine.clone();
        py.allow_threads(|| self.block_on(execute_impl(&engine, sql, params)))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_all(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyList>> {
        check_no_running_loop(py)?;
        let params = to_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        py.allow_threads(|| self.block_on(fetch_all_impl(&engine, &options, sql, params)))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_one(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        check_no_running_loop(py)?;
        let params = to_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        py.allow_threads(|| self.block_on(fetch_one_impl(&engine, &options, sql, params)))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_value(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<PyObject> {
        check_no_running_loop(py)?;
        let params = to_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        py.allow_threads(|| self.block_on(fetch_value_impl(&engine, &options, sql, params)))
    }

    #[pyo3(signature = (sql, params=None))]
    fn fetch_column(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyList>> {
        check_no_running_loop(py)?;
        let params = to_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        py.allow_threads(|| self.block_on(fetch_column_impl(&engine, &options, sql, params)))
    }

    /// Streaming sincrono por lotes (reemplaza `iter_dict_chunks`). Devuelve
    /// un `BlockingBatchStream` iterable con `for batch in ...`.
    #[pyo3(signature = (sql, params=None, batch_size=None))]
    fn stream(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Py<BlockingBatchStream>> {
        check_no_running_loop(py)?;
        let params = to_params(py, params)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        let batch_size = batch_size.unwrap_or(options.stream_batch_size);

        let (lease, cursor) =
            py.allow_threads(|| self.block_on(query_cursor_impl(&engine, sql, params)))?;

        let runtime = self.runtime.handle().clone();
        Py::new(
            py,
            BlockingBatchStream::new(lease, cursor, batch_size, options, runtime),
        )
    }

    #[pyo3(signature = (sql, rows))]
    fn executebatch(
        &self,
        py: Python<'_>,
        sql: String,
        rows: Bound<'_, PyAny>,
    ) -> PyResult<crate::bulk::BulkReport> {
        check_no_running_loop(py)?;
        let rows = crate::bulk::rows_to_param_values(py, &rows)?;
        let engine = self.engine.clone();
        let chunk_size = self.options.batch_size;
        py.allow_threads(|| self.block_on(executebatch_impl(&engine, sql, rows, chunk_size)))
    }

    #[pyo3(signature = (schema, proc, params=None))]
    fn call_proc(
        &self,
        py: Python<'_>,
        schema: String,
        proc: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Py<crate::proc::ProcResult>> {
        check_no_running_loop(py)?;
        let params_owned: Py<PyAny> = match params {
            Some(p) => p.unbind(),
            None => py.None(),
        };
        let engine = self.engine.clone();
        let strip = self.options.strip_char_padding;
        let decimal_mode = self.options.decimal_mode.clone();
        py.allow_threads(|| {
            self.block_on(call_proc_impl(
                &engine,
                schema,
                proc,
                params_owned,
                strip,
                decimal_mode,
            ))
        })
    }

    #[cfg(feature = "tablesync")]
    #[pyo3(signature = (source=None))]
    fn table_sync<'py>(
        &self,
        py: Python<'py>,
        source: Option<Py<BlockingEngine>>,
    ) -> crate::tablesync::TableSync {
        let runtime = self.runtime.handle().clone();
        crate::tablesync::TableSync::new(
            self.engine.clone(),
            source.map(|s| s.borrow(py).engine.clone()),
            self.options.merge_chunk_size,
            Some(runtime),
        )
    }
}

/// Iterador sincrono por lotes. Mismo `RowCursor` que el async, pero
/// `__next__` usa `runtime.block_on(fetch_batch)` -- RAM acotada igual.
#[pyclass(module = "rustodbc")]
pub struct BlockingBatchStream {
    // Se conserva el cursor en un Option para poder "tomarlo" por batch.
    cursor: Option<(Lease, crate::core::RowCursor)>,
    batch_size: usize,
    options: EngineOptions,
    columns_meta: Vec<crate::core::ffi::ColumnMeta>,
    columns: Vec<String>,
    runtime: tokio::runtime::Handle,
    done: bool,
}

impl BlockingBatchStream {
    fn new(
        lease: Lease,
        cursor: crate::core::RowCursor,
        batch_size: usize,
        options: EngineOptions,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let columns = cursor.column_names();
        let columns_meta = cursor.column_metas();
        BlockingBatchStream {
            cursor: Some((lease, cursor)),
            batch_size,
            options,
            columns_meta,
            columns,
            runtime,
            done: false,
        }
    }
}

#[pymethods]
impl BlockingBatchStream {
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyList>> {
        if self.done {
            return Err(PyStopIteration::new_err(()));
        }
        let Some((lease, cursor)) = self.cursor.take() else {
            self.done = true;
            return Err(PyStopIteration::new_err(()));
        };

        let batch_size = self.batch_size;
        let runtime = self.runtime.clone();
        let columns_meta = self.columns_meta.clone();
        let options = self.options.clone();

        let (lease, cursor, batch) = py.allow_threads(move || {
            runtime.block_on(async move {
                tokio::task::spawn_blocking(move || {
                    let mut cursor = cursor;
                    let batch = cursor.fetch_batch(batch_size);
                    (lease, cursor, batch)
                })
                .await
                .map_err(|e| to_py_err(CoreError::Connect(format!("panic: {e}"))))
            })
        })?;

        let batch = batch.map_err(to_py_err)?;

        if batch.is_empty() {
            self.done = true;
            // El lease se dropea aca (vuelve al pool).
            return Err(PyStopIteration::new_err(()));
        }

        self.cursor = Some((lease, cursor));

        batch_to_pylist(
            py,
            &columns_meta,
            &batch,
            options.strip_char_padding,
            &options.decimal_mode,
        )
    }
}

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
use crate::core::{ProcOutParams, ProcParam};
use crate::engine::{
    batch_execute_impl, call_proc_args_impl, call_proc_impl, connect_impl, execute_impl,
    executebatch_impl, fetch_all_impl, fetch_column_impl, fetch_one_impl, fetch_value_impl,
    parallel_execute_impl, query_cursor_impl, resolve_dsn,
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
        let engine = runtime.block_on(async move { connect_impl(dsn, opts_for_connect).await })?;

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
        py.allow_threads(move || self.block_on(execute_impl(engine, sql, params)))
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
        py.allow_threads(move || self.block_on(fetch_all_impl(engine, options, sql, params)))
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
        py.allow_threads(move || self.block_on(fetch_one_impl(engine, options, sql, params)))
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
        py.allow_threads(move || self.block_on(fetch_value_impl(engine, options, sql, params)))
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
        py.allow_threads(move || self.block_on(fetch_column_impl(engine, options, sql, params)))
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
            py.allow_threads(move || self.block_on(query_cursor_impl(engine, sql, params)))?;

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
        py.allow_threads(move || self.block_on(executebatch_impl(engine, sql, rows, chunk_size)))
    }

    #[pyo3(signature = (sql, rows, *, max_workers=None, fail_fast=false))]
    fn batch_execute(
        &self,
        py: Python<'_>,
        sql: String,
        rows: Bound<'_, PyAny>,
        max_workers: Option<usize>,
        fail_fast: bool,
    ) -> PyResult<crate::bulk::ParallelReport> {
        check_no_running_loop(py)?;
        let chunk_size = self.options.batch_size;
        // 2d: convertir a chunks una sola vez (sin clonar filas por worker).
        let chunks = crate::bulk::rows_to_chunks(py, &rows, chunk_size)?;
        let engine = self.engine.clone();
        let workers = max_workers.unwrap_or(self.options.max_workers);
        py.allow_threads(move || {
            self.block_on(batch_execute_impl(engine, sql, chunks, workers, fail_fast))
        })
    }

    #[pyo3(signature = (tasks, *, max_workers=None, fail_fast=false))]
    fn parallel_execute(
        &self,
        py: Python<'_>,
        tasks: Bound<'_, PyAny>,
        max_workers: Option<usize>,
        fail_fast: bool,
    ) -> PyResult<crate::bulk::ParallelReport> {
        check_no_running_loop(py)?;
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
        let engine = self.engine.clone();
        let chunk_size = self.options.batch_size;
        let workers = max_workers.unwrap_or(self.options.max_workers);
        py.allow_threads(move || {
            self.block_on(parallel_execute_impl(
                engine, tasks_vec, chunk_size, workers, fail_fast,
            ))
        })
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
        py.allow_threads(move || {
            self.block_on(call_proc_impl(
                engine,
                schema,
                proc,
                params_owned,
                strip,
                decimal_mode,
            ))
        })
    }

    /// Variante POSICIONAL de `call_proc` (secuencia en orden ordinal, sin
    /// nombres). Valida cada valor contra el tipo/largo declarado del parametro
    /// y levanta `ProcValidationError` si hay fallos. Ver `Db2iEngine`.
    #[pyo3(signature = (schema, proc, params=None))]
    fn call_proc_args(
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
        py.allow_threads(move || {
            self.block_on(call_proc_args_impl(
                engine,
                schema,
                proc,
                params_owned,
                strip,
                decimal_mode,
            ))
        })
    }

    /// Streaming sync de un `CALL` con params por NOMBRE: devuelve un
    /// `BlockingProcStream` iterable de `(set_index, list[Row])` por lotes.
    /// Los OUT/INOUT quedan en `stream.out_params` tras agotar.
    #[pyo3(signature = (schema, proc, params=None, batch_size=None))]
    fn call_proc_stream(
        &self,
        py: Python<'_>,
        schema: String,
        proc: String,
        params: Option<Bound<'_, PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Py<BlockingProcStream>> {
        use pyo3::types::PyDict;
        check_no_running_loop(py)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        let batch_size = batch_size.unwrap_or(options.stream_batch_size).max(1);

        // Lease con el GIL liberado.
        let lease: Lease = py.allow_threads(move || {
            self.block_on(async move { engine.acquire().await.map_err(to_py_err) })
        })?;

        // Metadata con el GIL liberado.
        let metadata: Vec<ProcParam> = py
            .allow_threads(|| lease.proc_columns(&schema, &proc))
            .map_err(to_py_err)?;
        if metadata.is_empty() {
            return Err(to_py_err(CoreError::Parameter(format!(
                "call_proc: no se encontro el procedimiento {schema}.{proc} en el catalogo \
                 (o no tiene parametros)"
            ))));
        }

        // Dict -> valores (con GIL, eager).
        let values = {
            let input: Option<Bound<'_, PyDict>> = match params {
                None => None,
                Some(p) => {
                    if p.is_none() {
                        None
                    } else {
                        let dict = p.downcast::<PyDict>().map_err(|_| {
                            to_py_err(CoreError::Parameter(
                                "call_proc: params debe ser un dict {nombre: valor} (o None)"
                                    .to_string(),
                            ))
                        })?;
                        Some(dict.clone())
                    }
                }
            };
            crate::proc::resolve_named_values(py, input.as_ref(), &metadata)?
        };

        // Cursor con el GIL liberado.
        let cursor = py
            .allow_threads(|| lease.call_proc_cursor(&schema, &proc, &metadata, &values))
            .map_err(to_py_err)?;

        let runtime = self.runtime.handle().clone();
        Py::new(
            py,
            BlockingProcStream::new(lease, cursor, metadata, batch_size, options, runtime),
        )
    }

    /// Streaming sync de un `CALL` POSICIONAL: devuelve un
    /// `BlockingProcStream` de `(set_index, list[Row])`. Valida eager
    /// (`ProcValidationError` en el call); OUT en `out_params` tras agotar.
    #[pyo3(signature = (schema, proc, params=None, batch_size=None))]
    fn call_proc_args_stream(
        &self,
        py: Python<'_>,
        schema: String,
        proc: String,
        params: Option<Bound<'_, PyAny>>,
        batch_size: Option<usize>,
    ) -> PyResult<Py<BlockingProcStream>> {
        check_no_running_loop(py)?;
        let engine = self.engine.clone();
        let options = self.options.clone();
        let batch_size = batch_size.unwrap_or(options.stream_batch_size).max(1);

        let lease: Lease = py.allow_threads(move || {
            self.block_on(async move { engine.acquire().await.map_err(to_py_err) })
        })?;

        let metadata: Vec<ProcParam> = py
            .allow_threads(|| lease.proc_columns(&schema, &proc))
            .map_err(to_py_err)?;
        if metadata.is_empty() {
            return Err(to_py_err(CoreError::Parameter(format!(
                "call_proc_args: no se encontro el procedimiento {schema}.{proc} en el catalogo \
                 (o no tiene parametros)"
            ))));
        }

        let values = match params {
            None => crate::proc::resolve_positional_values(
                py,
                &py.None().bind(py).clone(),
                &metadata,
                &schema,
                &proc,
            )?,
            Some(p) => crate::proc::resolve_positional_values(py, &p, &metadata, &schema, &proc)?,
        };

        let cursor = py
            .allow_threads(|| lease.call_proc_cursor(&schema, &proc, &metadata, &values))
            .map_err(to_py_err)?;

        let runtime = self.runtime.handle().clone();
        Py::new(
            py,
            BlockingProcStream::new(lease, cursor, metadata, batch_size, options, runtime),
        )
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

/// Resultado de avanzar de result set dentro del hilo bloqueante (mismo
/// patron que `proc_stream::AdvOutcome`; evita tuplas anidadas y closures
/// invocadas al toque).
enum BlockingAdvance {
    More {
        index: usize,
        columns: Vec<crate::core::ffi::ColumnMeta>,
        names: Vec<String>,
    },
    Finished(ProcOutParams),
}

/// Iterador sincrono por lotes sobre un `CALL` multi-result-set. Itera
/// `(set_index, list[Row])`; los sets vacios se saltan. `out_params` estricto
/// tras agotar. RAM acotada a un lote (el cursor nunca materializa todo).
#[pyclass(module = "rustodbc")]
pub struct BlockingProcStream {
    cursor: Option<(Lease, crate::core::ProcCursor)>,
    metadata: Vec<ProcParam>,
    batch_size: usize,
    options: EngineOptions,
    runtime: tokio::runtime::Handle,
    current_columns_meta: Vec<crate::core::ffi::ColumnMeta>,
    current_columns: Vec<String>,
    current_set_index: usize,
    out_params: Option<ProcOutParams>,
    done: bool,
}

impl BlockingProcStream {
    fn new(
        lease: Lease,
        cursor: crate::core::ProcCursor,
        metadata: Vec<ProcParam>,
        batch_size: usize,
        options: EngineOptions,
        runtime: tokio::runtime::Handle,
    ) -> Self {
        let current_columns = cursor.column_names();
        let current_columns_meta = cursor.column_metas();
        let current_set_index = cursor.set_index();
        BlockingProcStream {
            cursor: Some((lease, cursor)),
            metadata,
            batch_size: batch_size.max(1),
            options,
            runtime,
            current_columns_meta,
            current_columns,
            current_set_index,
            out_params: None,
            done: false,
        }
    }
}

#[pymethods]
impl BlockingProcStream {
    /// Columnas del result set actual.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.current_columns.clone()
    }

    /// Indice (0-based) del result set actual entre los entregados.
    #[getter]
    fn set_index(&self) -> usize {
        self.current_set_index
    }

    /// OUT/INOUT del procedimiento. Solo tras agotar; si no, `InterfaceError`.
    #[getter]
    fn out_params(&self, py: Python<'_>) -> PyResult<Py<pyo3::types::PyDict>> {
        let Some(out) = (if self.done {
            self.out_params.clone()
        } else {
            None
        }) else {
            return Err(to_py_err(CoreError::Interface(
                "call_proc_stream: out_params solo disponible tras agotar el stream \
                 (drenar todos los result sets)"
                    .to_string(),
            )));
        };
        crate::proc::out_params_to_pydict(
            py,
            &self.metadata,
            &out,
            self.options.strip_char_padding,
            &self.options.decimal_mode,
        )
    }

    /// Cierra el stream sin agotar: libera el statement y devuelve la
    /// conexion al pool. `out_params` queda no disponible (estricto).
    fn close(&mut self) {
        self.done = true;
        self.cursor = None;
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        use pyo3::types::PyTuple;

        if self.done {
            return Err(PyStopIteration::new_err(()));
        }
        loop {
            let Some((lease, cursor)) = self.cursor.take() else {
                self.done = true;
                return Err(PyStopIteration::new_err(()));
            };

            let batch_size = self.batch_size;
            let runtime = self.runtime.clone();

            // 1. Fetch del set actual (GIL liberado).
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
            if !batch.is_empty() {
                let columns_meta = self.current_columns_meta.clone();
                let set_index = self.current_set_index;
                let options = self.options.clone();
                self.cursor = Some((lease, cursor));
                let pylist = batch_to_pylist(
                    py,
                    &columns_meta,
                    &batch,
                    options.strip_char_padding,
                    &options.decimal_mode,
                )?;
                let tuple = PyTuple::new_bound(py, [set_index.into_py(py), pylist.into_py(py)]);
                return Ok(tuple.into_any().unbind());
            }

            // 2. Set agotado: avanzar (GIL liberado).
            let runtime = self.runtime.clone();
            let (lease, cursor, advanced) = py.allow_threads(move || {
                runtime.block_on(async move {
                    tokio::task::spawn_blocking(move || {
                        let mut cursor = cursor;
                        let res = match cursor.advance() {
                            Err(e) => Err(e),
                            Ok(true) => Ok(BlockingAdvance::More {
                                index: cursor.set_index(),
                                columns: cursor.column_metas(),
                                names: cursor.column_names(),
                            }),
                            Ok(false) => {
                                let out = cursor.take_out_params().unwrap_or_default();
                                Ok(BlockingAdvance::Finished(out))
                            }
                        };
                        (lease, cursor, res)
                    })
                    .await
                    .map_err(|e| to_py_err(CoreError::Connect(format!("panic: {e}"))))
                })
            })?;
            match advanced.map_err(to_py_err)? {
                BlockingAdvance::More {
                    index,
                    columns,
                    names,
                } => {
                    self.current_set_index = index;
                    self.current_columns_meta = columns;
                    self.current_columns = names;
                    self.cursor = Some((lease, cursor));
                    continue;
                }
                BlockingAdvance::Finished(out) => {
                    self.out_params = Some(out);
                    self.done = true;
                    // Lease se dropea aca (vuelve al pool sano).
                    drop((lease, cursor));
                    return Err(PyStopIteration::new_err(()));
                }
            }
        }
    }
}

//! `ProcStream`: iterador async **por lote** sobre un `CALL` multi-result-set.
//!
//! Misma arquitectura que `BatchStream` (`stream.rs`): al crear el stream se
//! spawna una tarea tokio que es DUENA del `(Lease, ProcCursor)` y va
//! drenando set por set, mandando eventos por un canal `mpsc` de capacidad
//! `prefetch_batches`. El fetch bloqueante corre en `spawn_blocking`; en cada
//! paso la tarea atiende un canal de cancelacion con `SQLCancel` real.
//!
//! Protocolo del canal (`ProcEvent`):
//! - `ResultSet{index, columns, names}`: arranca un set (solo sets con
//!   columnas; los vacios se saltan en el cursor, paridad con `ProcResult`).
//! - `Batch(filas)`: lote no vacio del set actual.
//! - `Done(out_params)`: se agotaron todos los sets; los OUT/INOUT ya leidos
//!   viajan aca (solo validos tras drenar todo, por spec ODBC).
//!
//! **Fin del stream: manda el canal, nunca un flag.** El productor manda
//! `Done` como ultimo evento y despues suelta `tx`; el consumidor termina con
//! el `Done` o con `recv() -> None`. Un flag compartido "ya termine" NO sirve:
//! el productor puede setearlo mientras el `Done` (y los ultimos lotes) sigue
//! encolado, y el `__anext__` que lo honra sale sin leerlo -- se pierden los
//! OUT y los ultimos lotes (repro: proc rapido, SQP06208).
//!
//! `cancelled` es **solo del consumidor** (`cancel()`/`aclose()`): el
//! productor jamas lo toca.
//!
//! `__anext__` devuelve `(set_index, list[Row])` plano: los lotes del mismo
//! set son contiguos, asi que el consumidor que quiera agrupar por set lo
//! hace con `itertools.groupby` sin que Rust pague una capa anidada. Los sets
//! con columnas pero 0 filas no producen lotes (se saltan; no hay filas que
//! entregar).
//!
//! RAM acotada a `prefetch_batches` eventos en vuelo + el lote en consumo.
//! `out_params` es ESTRICTO: si el stream no se agoto, levanta
//! `InterfaceError` (nunca parcial ni vacio silencioso).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use tokio::sync::Mutex;

use crate::config::EngineOptions;
use crate::core::ffi::ColumnMeta;
use crate::core::{ColumnValue, Lease, ProcCursor, ProcOutParams, ProcParam};
use crate::errors::{to_py_err, CoreError};
use crate::proc::out_params_to_pydict;
use crate::rows::batch_to_pylist;

/// Evento del canal de prefetch. Ver doc del modulo.
enum ProcEvent {
    ResultSet {
        index: usize,
        columns: Vec<ColumnMeta>,
        names: Vec<String>,
    },
    Batch(Vec<Vec<ColumnValue>>),
    Done(ProcOutParams),
}

type PrefetchItem = Result<ProcEvent, CoreError>;

/// Resultado del `advance()` dentro del hilo bloqueante.
enum AdvOutcome {
    More {
        index: usize,
        columns: Vec<ColumnMeta>,
        names: Vec<String>,
    },
    Finished(ProcOutParams),
}

/// Estado mutable del stream (columnas del set actual + OUT al final).
/// `std::Mutex` (no tokio): los locks son cortos y los getters sync
/// (`columns`/`out_params`) tambien lo necesitan.
struct ProcStreamState {
    columns_meta: Vec<ColumnMeta>,
    columns: Vec<String>,
    set_index: usize,
    out_params: Option<ProcOutParams>,
    done: bool,
}

#[pyclass(module = "rustodbc")]
pub struct ProcStream {
    rx: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<PrefetchItem>>>>,
    cancel_tx: Arc<tokio::sync::mpsc::Sender<()>>,
    task: Arc<tokio::task::JoinHandle<()>>,
    options: EngineOptions,
    /// Metadata del catalogo (para convertir OUT por SQL type en el getter).
    metadata: Vec<ProcParam>,
    state: Arc<std::sync::Mutex<ProcStreamState>>,
    /// Solo lo setean `cancel()`/`aclose()` (decision del consumidor). El
    /// productor NUNCA lo toca -- ver la nota de fin de stream en el doc del
    /// modulo.
    cancelled: Arc<AtomicBool>,
}

impl ProcStream {
    pub fn new(
        lease: Lease,
        cursor: ProcCursor,
        metadata: Vec<ProcParam>,
        batch_size: usize,
        prefetch_batches: usize,
        options: EngineOptions,
    ) -> Self {
        let batch_size = batch_size.max(1);
        let init_meta = cursor.column_metas();
        let init_names = cursor.column_names();
        let state = Arc::new(std::sync::Mutex::new(ProcStreamState {
            columns_meta: init_meta,
            columns: init_names,
            set_index: 0,
            out_params: None,
            done: false,
        }));

        let capacity = prefetch_batches.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        let cancelled = Arc::new(AtomicBool::new(false));

        let task = tokio::task::spawn(async move {
            let mut lease = Some(lease);
            let cursor = Arc::new(std::sync::Mutex::new(cursor));

            // Cancelacion pedida por el consumidor: SQLCancel REAL + consumir
            // la conexion + terminar la tarea.
            macro_rules! cancel_and_discard {
                () => {{
                    if let Ok(guard) = cursor.lock() {
                        let _ = guard.cancel();
                    }
                    drop(cursor);
                    if let Some(l) = lease.take() {
                        drop(l.take_connection());
                    }
                    return;
                }};
            }

            // Envio cancelable: si el consumidor esta parado (canal lleno) y
            // pide cancelar, hay que ejecutar el SQLCancel igual. Sin esto la
            // tarea queda parkeada en `send` y la rama de cancelacion nunca
            // corre.
            macro_rules! send_or_cancel {
                ($item:expr) => {
                    tokio::select! {
                        r = tx.send($item) => r.is_ok(),
                        _ = cancel_rx.recv() => { cancel_and_discard!(); }
                    }
                };
            }

            // Evento inicial: primer set o Done directo (proc sin sets).
            {
                let (is_done, idx, cols, names, out) = {
                    let mut guard = cursor.lock().unwrap();
                    if guard.is_done() {
                        let out = guard.take_out_params().unwrap_or_default();
                        (true, 0usize, Vec::new(), Vec::new(), out)
                    } else {
                        let idx = guard.set_index();
                        let cols = guard.column_metas();
                        let names = guard.column_names();
                        (false, idx, cols, names, Vec::new())
                    }
                };
                if is_done {
                    let _ = send_or_cancel!(Ok(ProcEvent::Done(out)));
                    drop(cursor);
                    if let Some(l) = lease.take() {
                        drop(l);
                    }
                    return;
                }
                if !send_or_cancel!(Ok(ProcEvent::ResultSet {
                    index: idx,
                    columns: cols,
                    names,
                })) {
                    return;
                }
            }

            loop {
                // 1. Fetch del set actual (bloqueante, cancelable).
                let cursor_for_fetch = cursor.clone();
                let fetched: Result<Vec<Vec<ColumnValue>>, CoreError> = tokio::select! {
                    batch = tokio::task::spawn_blocking(move || {
                        let mut guard = cursor_for_fetch.lock().unwrap();
                        guard.fetch_batch(batch_size)
                    }) => {
                        match batch {
                            Ok(b) => b,
                            Err(e) => Err(CoreError::Connect(format!("panic: {e}"))),
                        }
                    }
                    _ = cancel_rx.recv() => { cancel_and_discard!(); }
                };

                match fetched {
                    Err(e) => {
                        // Como `BatchStream`: se entrega el error pero el
                        // stream sigue usable.
                        if !send_or_cancel!(Err(e)) {
                            return;
                        }
                        continue;
                    }
                    Ok(batch) => {
                        if !batch.is_empty() {
                            if !send_or_cancel!(Ok(ProcEvent::Batch(batch))) {
                                return;
                            }
                            continue;
                        }
                        // 2. Set agotado: avanzar (bloqueante, cancelable).
                        let cursor_for_adv = cursor.clone();
                        let advanced: Result<AdvOutcome, CoreError> = tokio::select! {
                            adv = tokio::task::spawn_blocking(move || {
                                let mut guard = cursor_for_adv.lock().unwrap();
                                match guard.advance() {
                                    Err(e) => Err(e),
                                    Ok(true) => Ok(AdvOutcome::More {
                                        index: guard.set_index(),
                                        columns: guard.column_metas(),
                                        names: guard.column_names(),
                                    }),
                                    Ok(false) => {
                                        let out =
                                            guard.take_out_params().unwrap_or_default();
                                        Ok(AdvOutcome::Finished(out))
                                    }
                                }
                            }) => {
                                match adv {
                                    Ok(b) => b,
                                    Err(e) => Err(CoreError::Connect(format!("panic: {e}"))),
                                }
                            }
                            _ = cancel_rx.recv() => { cancel_and_discard!(); }
                        };
                        match advanced {
                            Err(e) => {
                                // Error de posicion: terminar (posicion
                                // desconocida, no seguir).
                                let _ = send_or_cancel!(Err(e));
                                drop(cursor);
                                if let Some(l) = lease.take() {
                                    drop(l);
                                }
                                return;
                            }
                            Ok(AdvOutcome::More {
                                index,
                                columns,
                                names,
                            }) => {
                                if !send_or_cancel!(Ok(ProcEvent::ResultSet {
                                    index,
                                    columns,
                                    names,
                                })) {
                                    return;
                                }
                            }
                            Ok(AdvOutcome::Finished(out)) => {
                                let _ = send_or_cancel!(Ok(ProcEvent::Done(out)));
                                drop(cursor);
                                if let Some(l) = lease.take() {
                                    drop(l);
                                }
                                return;
                            }
                        }
                    }
                }
            }
        });

        ProcStream {
            rx: Arc::new(Mutex::new(Some(rx))),
            cancel_tx: Arc::new(cancel_tx),
            task: Arc::new(task),
            options,
            metadata,
            state,
            cancelled,
        }
    }
}

#[pymethods]
impl ProcStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Columnas del result set actual.
    #[getter]
    fn columns(&self) -> Vec<String> {
        self.state.lock().unwrap().columns.clone()
    }

    /// Indice (0-based) del result set actual entre los entregados.
    #[getter]
    fn set_index(&self) -> usize {
        self.state.lock().unwrap().set_index
    }

    /// OUT/INOUT del procedimiento. Solo disponible tras agotar el stream;
    /// si no, levanta `InterfaceError` (nunca parcial).
    #[getter]
    fn out_params(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
        let out = {
            let guard = self.state.lock().unwrap();
            if guard.done {
                guard.out_params.clone()
            } else {
                None
            }
        };
        let Some(out) = out else {
            return Err(to_py_err(CoreError::Interface(
                "call_proc_stream: out_params solo disponible tras agotar el stream \
                 (drenar todos los result sets)"
                    .to_string(),
            )));
        };
        out_params_to_pydict(
            py,
            &self.metadata,
            &out,
            self.options.strip_char_padding,
            &self.options.decimal_mode,
        )
    }

    fn __anext__<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let cancelled = self.cancelled.clone();
        let options = self.options.clone();
        let state = self.state.clone();
        let wait_secs = options.query_timeout;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if cancelled.load(Ordering::SeqCst) {
                return Err(PyStopAsyncIteration::new_err(()));
            }

            loop {
                let receiver = {
                    let mut guard = rx.lock().await;
                    guard.take()
                };
                let Some(mut receiver) = receiver else {
                    return Err(PyStopAsyncIteration::new_err(()));
                };

                let recv_fut = receiver.recv();
                let event = if wait_secs > 0 {
                    match tokio::time::timeout(
                        std::time::Duration::from_secs(wait_secs as u64),
                        recv_fut,
                    )
                    .await
                    {
                        Ok(b) => b,
                        Err(_) => {
                            *rx.lock().await = Some(receiver);
                            return Err(to_py_err(CoreError::Interface(format!(
                                "call_proc_stream: timeout de {}s esperando el lote",
                                wait_secs
                            ))));
                        }
                    }
                } else {
                    recv_fut.await
                };

                let event = match event {
                    Some(b) => b,
                    None => {
                        // Canal cerrado: la tarea termino y ya entrego TODO lo
                        // encolado (o se aborto). Fin normal.
                        return Err(PyStopAsyncIteration::new_err(()));
                    }
                };

                match event {
                    Err(e) => {
                        // Reinsertar: el stream sigue usable tras un error de
                        // fetch (igual que `BatchStream`).
                        *rx.lock().await = Some(receiver);
                        return Err(to_py_err(e));
                    }
                    Ok(ProcEvent::ResultSet {
                        index,
                        columns,
                        names,
                    }) => {
                        {
                            let mut guard = state.lock().unwrap();
                            guard.columns_meta = columns;
                            guard.columns = names;
                            guard.set_index = index;
                        }
                        *rx.lock().await = Some(receiver);
                        // Sets vacios (sin Batch posterior) se saltan: seguir
                        // esperando el siguiente evento sin yield.
                        continue;
                    }
                    Ok(ProcEvent::Batch(rows)) => {
                        let (cols, idx, opts) = {
                            let guard = state.lock().unwrap();
                            (guard.columns_meta.clone(), guard.set_index, options.clone())
                        };
                        *rx.lock().await = Some(receiver);
                        return Python::with_gil(|py| {
                            let pylist = batch_to_pylist(
                                py,
                                &cols,
                                &rows,
                                opts.strip_char_padding,
                                &opts.decimal_mode,
                            )?;
                            let tuple =
                                PyTuple::new_bound(py, [idx.into_py(py), pylist.into_py(py)]);
                            Ok(tuple.into_any().unbind())
                        });
                    }
                    Ok(ProcEvent::Done(out)) => {
                        {
                            let mut guard = state.lock().unwrap();
                            guard.out_params = Some(out);
                            guard.done = true;
                        }
                        // No reinsertar: la tarea termino y el canal se
                        // cierra; el proximo __anext__ sale por receiver
                        // ausente. Los OUT ya quedaron en `state`.
                        return Err(PyStopAsyncIteration::new_err(()));
                    }
                }
            }
        })
    }

    /// Cancela el stream: pide a la tarea un `SQLCancel` REAL sobre el
    /// statement y CONSUMIR la conexion (nunca vuelve al pool).
    ///
    /// NO se dropea el receiver aca: si se dropeara, el `send` pendiente de la
    /// tarea fallaria y la tarea saldria por ese camino SIN ejecutar el
    /// `SQLCancel`. La senal (`cancel_tx`) es la unica via.
    fn cancel(&self) -> PyResult<()> {
        self.cancelled.store(true, Ordering::SeqCst);
        let _ = self.cancel_tx.try_send(());
        Ok(())
    }

    /// Corta el stream ya: aborta la tarea (no busca un `SQLCancel` ordenado
    /// -- para eso esta `cancel()`) y cierra el canal si no esta en uso.
    fn aclose<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Ok(mut guard) = self.rx.try_lock() {
            *guard = None;
        }
        self.task.abort();
        pyo3_async_runtimes::tokio::future_into_py(py, async move { Ok(()) })
    }

    fn __aenter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &mut self,
        py: Python<'py>,
        _exc_type: Option<Bound<'py, PyAny>>,
        _exc_value: Option<Bound<'py, PyAny>>,
        _traceback: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.aclose(py)
    }
}

//! `BatchStream`: iterador async **por lote** (no por fila) con **prefetch**.
//!
//! Al crear el stream se spawna una tarea tokio que es DUEÑA del `(Lease,
//! RowCursor)` y va drenando el cursor en lotes de `batch_size`, mandandolos
//! por un canal `mpsc` de capacidad `prefetch_batches`. El fetch bloqueante
//! corre en `spawn_blocking` (no ocupa un worker async); entre lotes la tarea
//! atiende un canal de cancelacion: `cancel()` dispara un `SQLCancel` real
//! sobre el statement y corta el drenado.
//!
//! **Fin del stream: manda el canal, nunca un flag.** El productor manda el
//! centinela `Ok(vec![])` como ultimo item y despues suelta `tx`. El
//! consumidor termina cuando recibe el centinela o cuando `recv()` devuelve
//! `None` (canal cerrado). Como el canal es FIFO y el centinela va DESPUES de
//! todos los lotes reales, nada encolado se pierde. Un flag compartido con
//! "ya termine" NO sirve: el productor puede setearlo mientras todavia hay
//! lotes en el canal, y el `__anext__` que lo honra pierde el ultimo lote.
//!
//! `cancelled` es **solo del consumidor** (`cancel()`/`aclose()`): el
//! productor jamas lo toca. Sirve para que `__anext__` corte ya aunque la
//! tarea este trabada en un ODBC que `SQLCancel` todavia no desbloqueo.
//!
//! `__anext__` recibe del canal: mientras Python consume el lote actual, la
//! tarea ya pidio el siguiente al driver. El `Lease` no vuelve al pool hasta
//! que la tarea termina (agotado, cancelado o canal cerrado), asi el `HStmt`
//! del cursor nunca queda vivo en una conexion reusada.
//!
//! RAM acotada a `prefetch_batches` lotes en vuelo (default 2) + el lote que
//! Python esta consumiendo -- no materializa el result set completo.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::Mutex;

use pyo3::exceptions::PyStopAsyncIteration;
use pyo3::prelude::*;

use crate::config::EngineOptions;
use crate::core::ffi::ColumnMeta;
use crate::core::{ColumnValue, Lease, RowCursor};
use crate::errors::{to_py_err, CoreError};
use crate::rows::batch_to_pylist;

/// Resultado enviado por la tarea de prefetch. `Ok(vec![])` = result set
/// agotado.
type PrefetchItem = Result<Vec<Vec<ColumnValue>>, crate::errors::CoreError>;

#[pyclass(module = "rustodbc")]
pub struct BatchStream {
    /// Canal de lotes ya traidos del driver. Capacidad = `prefetch_batches`.
    /// Compartido en un `Mutex` para persistir el receiver entre llamadas a
    /// `__anext__`: cada llamada lo toma, espera un lote y lo reinserta. El
    /// `future_into_py` exige `'static`, asi que no se puede prestar
    /// `&mut self.rx` -- el `Arc<Mutex>` es el puente.
    rx: Arc<Mutex<Option<tokio::sync::mpsc::Receiver<PrefetchItem>>>>,
    /// Canal para pedirle a la tarea de prefetch que cancele el statement.
    cancel_tx: Arc<tokio::sync::mpsc::Sender<()>>,
    /// Handle de la tarea que sostiene `(Lease, RowCursor)`. Se aborta al
    /// cerrar para que el Lease vuelva al pool.
    task: Arc<tokio::task::JoinHandle<()>>,
    options: EngineOptions,
    /// Metadata de columnas (necesaria para convertir lotes a `list[dict]`; la
    /// tarea tiene el cursor, este pyclass conserva la metadata).
    columns_meta: Vec<ColumnMeta>,
    columns: Vec<String>,
    /// Solo lo setean `cancel()`/`aclose()` (decision del consumidor). El
    /// productor NUNCA lo toca -- ver la nota de fin de stream en el doc del
    /// modulo.
    cancelled: Arc<AtomicBool>,
}

impl BatchStream {
    pub fn new(
        lease: Lease,
        cursor: RowCursor,
        batch_size: usize,
        prefetch_batches: usize,
        options: EngineOptions,
    ) -> Self {
        let columns = cursor.column_names();
        let columns_meta = cursor.column_metas();
        let capacity = prefetch_batches.max(1);
        let (tx, rx) = tokio::sync::mpsc::channel(capacity);
        let (cancel_tx, mut cancel_rx) = tokio::sync::mpsc::channel::<()>(1);
        let cancelled = Arc::new(AtomicBool::new(false));

        // La tarea drena el cursor en spawn_blocking (el SQLFetch es
        // bloqueante) y atiende la cancelacion en cada paso: al recibir la
        // senal, hace `SQLCancel` REAL sobre el statement (el cursor vive en
        // un `Arc<Mutex>` compartido) y CONSUME la conexion permanentemente
        // (`take_connection`) -- el SQLDisconnect corta cualquier resto y la
        // conexion nunca vuelve al pool (regla AGENTS.md ss4). Cuando el
        // receiver se dropea (stream cerrado), `send` falla y la tarea sale
        // igual.
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

            loop {
                let cursor_for_fetch = cursor.clone();
                let fetched = tokio::select! {
                    batch = tokio::task::spawn_blocking(move || {
                        // std Mutex en hilo de blocking: lock corto, se suelta
                        // al terminar fetch_batch.
                        let mut guard = cursor_for_fetch.lock().unwrap();
                        guard.fetch_batch(batch_size)
                    }) => {
                        match batch {
                            Ok(b) => b,
                            Err(e) => Err(crate::errors::CoreError::Connect(format!(
                                "panic: {e}"
                            ))),
                        }
                    }
                    _ = cancel_rx.recv() => { cancel_and_discard!(); }
                };

                let is_exhausted = matches!(&fetched, Ok(b) if b.is_empty());
                // El ENVIO tambien es cancelable: si el consumidor esta
                // parado (canal lleno) y pide cancelar, hay que ejecutar el
                // SQLCancel igual. Sin esto la tarea queda parkeada en `send`
                // y la rama de cancelacion nunca corre.
                let sent = tokio::select! {
                    r = tx.send(fetched) => r.is_ok(),
                    _ = cancel_rx.recv() => { cancel_and_discard!(); }
                };
                if !sent {
                    return; // consumidor cerrado: lease/cursor se dropean al salir
                }
                if is_exhausted {
                    drop(cursor);
                    if let Some(l) = lease.take() {
                        drop(l); // vuelve al pool sano
                    }
                    return;
                }
            }
        });

        BatchStream {
            rx: Arc::new(Mutex::new(Some(rx))),
            cancel_tx: Arc::new(cancel_tx),
            task: Arc::new(task),
            options,
            columns_meta,
            columns,
            cancelled,
        }
    }
}

#[pymethods]
impl BatchStream {
    fn __aiter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[getter]
    fn columns(&self) -> Vec<String> {
        self.columns.clone()
    }

    fn __anext__<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let rx = self.rx.clone();
        let cancelled = self.cancelled.clone();
        let options = self.options.clone();
        let columns_meta = self.columns_meta.clone();

        // Timeout opcional esperando el lote (query_timeout segundos; 0 = sin
        // timeout). Si el driver esta trabado en un fetch en curso, el timeout
        // devuelve error al consumidor; aclose()/cancel() cortan la tarea.
        let wait_secs = options.query_timeout;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if cancelled.load(Ordering::SeqCst) {
                return Err(PyStopAsyncIteration::new_err(()));
            }

            // Tomar el receiver del mutex compartido. Se reinserta al final
            // (o se dropea en los caminos de fin, lo que hace que la tarea de
            // prefetch vea `send` fallar y suelte el Lease).
            let receiver = {
                let mut guard = rx.lock().await;
                guard.take()
            };
            let Some(mut receiver) = receiver else {
                return Err(PyStopAsyncIteration::new_err(()));
            };

            let recv_fut = receiver.recv();
            let batch = if wait_secs > 0 {
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
                            "stream_batches: timeout de {}s esperando el lote",
                            wait_secs
                        ))));
                    }
                }
            } else {
                recv_fut.await
            };

            let batch = match batch {
                Some(b) => b,
                None => {
                    // Canal cerrado: la tarea termino y ya entrego TODO lo
                    // encolado (o se aborto). Fin normal.
                    return Err(PyStopAsyncIteration::new_err(()));
                }
            };

            let is_empty = matches!(&batch, Ok(b) if b.is_empty());
            if is_empty {
                // Centinela del productor: todo lo anterior ya se entrego.
                return Err(PyStopAsyncIteration::new_err(()));
            }

            // Reinsertar el receiver para la siguiente iteracion.
            *rx.lock().await = Some(receiver);

            let batch = batch.map_err(to_py_err)?;
            Python::with_gil(|py| {
                batch_to_pylist(
                    py,
                    &columns_meta,
                    &batch,
                    options.strip_char_padding,
                    &options.decimal_mode,
                )
            })
        })
    }

    /// Cancela el stream: pide a la tarea de prefetch un `SQLCancel` REAL
    /// sobre el statement y CONSUMIR la conexion (nunca vuelve al pool).
    /// Seguro de llamar mientras un `__anext__` esta en curso; el proximo
    /// `__anext__` sale con StopAsyncIteration.
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

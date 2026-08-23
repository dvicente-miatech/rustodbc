//! `BatchStream`: iterador async **por lote** (no por fila) con **prefetch**.
//!
//! Al crear el stream se spawna una tarea tokio que es DUEÑA del `(Lease,
//! RowCursor)` y va drenando el cursor en lotes de `batch_size`, mandandolos
//! por un canal `mpsc` de capacidad `prefetch_batches`. El fetch bloqueante
//! corre en `spawn_blocking` (no ocupa un worker async); entre lotes la tarea
//! atiende un canal de cancelacion: `cancel()` dispara un `SQLCancel` real
//! sobre el statement y corta el drenado.
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

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
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
    /// Metadata de columnas (necesaria para convertir lotes a `list[dict]`;
    /// la tarea tiene el cursor, este pyclass conserva la metadata).
    columns_meta: Vec<ColumnMeta>,
    columns: Vec<String>,
    exhausted: Arc<AtomicBool>,
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
        let exhausted = Arc::new(AtomicBool::new(false));
        let exhausted_2 = exhausted.clone();

        // La tarea drena el cursor en spawn_blocking (el SQLFetch es
        // bloqueante) y entre lotes atiende la cancelacion: al recibir la
        // senal, hace `SQLCancel` REAL sobre el statement (el cursor vive en
        // un `Arc<Mutex>` compartido entre la tarea y la senal) y CONSUME la
        // conexion permanentemente (`take_connection`) -- el SQLDisconnect
        // corta cualquier resto y la conexion nunca vuelve al pool (regla
        // AGENTS.md ss4). Cuando el receiver se dropea (stream cerrado),
        // `send` falla y la tarea sale igual.
        let task = tokio::task::spawn(async move {
            let mut lease = Some(lease);
            let cursor = Arc::new(std::sync::Mutex::new(cursor));
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
                    _ = cancel_rx.recv() => {
                        // Cancelacion pedida por el consumidor: SQLCancel REAL
                        // sobre el statement + CONSUMIR la conexion.
                        if let Ok(mut guard) = cursor.lock() {
                            let _ = guard.cancel();
                        }
                        drop(cursor);
                        if let Some(l) = lease.take() {
                            drop(l.take_connection());
                        }
                        return;
                    }
                };

                let is_exhausted = matches!(&fetched, Ok(b) if b.is_empty());
                if tx.send(fetched).await.is_err() {
                    return; // lease/cursor se dropean al salir
                }
                if is_exhausted {
                    exhausted_2.store(true, Ordering::SeqCst);
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
            exhausted,
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
        let exhausted = self.exhausted.clone();
        let options = self.options.clone();
        let columns_meta = self.columns_meta.clone();

        // Timeout opcional esperando el lote (query_timeout segundos; 0 =
        // sin timeout). Si el driver esta trabado en un fetch en curso, el
        // timeout devuelve error al consumidor; aclose()/cancel() cortan la
        // tarea.
        let wait_secs = options.query_timeout;

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if exhausted.load(Ordering::SeqCst) {
                return Err(PyStopAsyncIteration::new_err(()));
            }

            // Tomar el receiver del mutex compartido. Se reinserta al final
            // (o se dropea en los caminos de error/agotado, lo que hace que
            // la tarea de prefetch vea `send` fallar y suelte el Lease).
            let mut receiver = {
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
                    // Canal cerrado (tarea abortada o stream cerrado).
                    exhausted.store(true, Ordering::SeqCst);
                    return Err(PyStopAsyncIteration::new_err(()));
                }
            };

            let is_empty = matches!(&batch, Ok(b) if b.is_empty());
            if is_empty {
                exhausted.store(true, Ordering::SeqCst);
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
    fn cancel(&self) -> PyResult<()> {
        self.exhausted.store(true, Ordering::SeqCst);
        // La tarea atiende la senal entre lotes (o al terminar el fetch en
        // curso): hace SQLCancel y consume la conexion. NO se aborta la
        // tarea aca -- el abort impediria que ejecute el SQLCancel.
        let _ = self.cancel_tx.try_send(());
        if let Ok(mut guard) = self.rx.try_lock() {
            *guard = None; // el proximo __anext__ sale por canal cerrado
        }
        Ok(())
    }

    fn aclose<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.exhausted.store(true, Ordering::SeqCst);
        // Cerrar el canal: dropear el receiver (si no esta en uso por un
        // `__anext__` en curso). La tarea de prefetch ve `send` fallar y
        // suelta el Lease.
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

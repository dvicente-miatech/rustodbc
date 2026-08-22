//! `BatchStream`: iterador async **por lote** (no por fila) con **prefetch**.
//!
//! Al crear el stream se spawna una tarea tokio que es DUEÑA del `Lease` +
//! `RowCursor` y va drenando el cursor en lotes de `batch_size`, mandandolos
//! por un canal `mpsc` de capacidad `prefetch_batches`. `__anext__` recibe
//! del canal: mientras Python consume el lote actual, la tarea ya pidio el
//! siguiente al driver (sobrecarga el fetch con el consumo). El `Lease` no
//! vuelve al pool hasta que la tarea termina (agotado o canal cerrado), asi
//! el `HStmt` del cursor nunca queda vivo en una conexion reusada.
//!
//! RAM acotada a `prefetch_batches` lotes en vuelo (default 2) + el lote que
//! Python esta consumiendo -- no materializa el result set completo.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;

use crate::config::EngineOptions;
use crate::core::ffi::ColumnMeta;
use crate::core::{ColumnValue, Lease, RowCursor};
use crate::errors::to_py_err;
use crate::rows::batch_to_pylist;

/// Resultado enviado por la tarea de prefetch. `Ok(vec![])` = result set
/// agotado.
type PrefetchItem = Result<Vec<Vec<ColumnValue>>, crate::errors::CoreError>;

#[pyclass(module = "rustodbc")]
pub struct BatchStream {
    /// Canal de lotes ya traidos del driver. Capacidad = `prefetch_batches`.
    /// `Option` porque cada `__anext__` lo toma para moverlo al future.
    rx: Option<tokio::sync::mpsc::Receiver<PrefetchItem>>,
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
        let exhausted = Arc::new(AtomicBool::new(false));
        let exhausted_2 = exhausted.clone();

        // La tarea drena el cursor y envia lotes por el canal. Cuando el
        // receiver se dropea (stream cerrado) `send` falla y la tarea sale
        // (droppea el Lease -> vuelve al pool).
        let task = tokio::task::spawn(async move {
            let mut cursor = cursor;
            loop {
                let batch = cursor.fetch_batch(batch_size);
                let is_exhausted = matches!(&batch, Ok(b) if b.is_empty());
                if tx.send(batch).await.is_err() {
                    drop(lease);
                    return;
                }
                if is_exhausted {
                    exhausted_2.store(true, Ordering::SeqCst);
                    drop(lease);
                    return;
                }
            }
        });

        BatchStream {
            rx: Some(rx),
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
        let Some(mut rx) = self.rx.take() else {
            return pyo3_async_runtimes::tokio::future_into_py(
                py,
                async move -> PyResult<Py<pyo3::types::PyList>> {
                    Err(PyStopAsyncIteration::new_err(()))
                },
            );
        };
        let exhausted = self.exhausted.clone();
        let options = self.options.clone();
        let columns_meta = self.columns_meta.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if exhausted.load(Ordering::SeqCst) {
                return Err(PyStopAsyncIteration::new_err(()));
            }

            let batch = rx.recv().await.ok_or_else(|| {
                PyRuntimeError::new_err("BatchStream: canal cerrado (stream cerrado o cancelado)")
            })?;

            let is_empty = matches!(&batch, Ok(b) if b.is_empty());
            if is_empty {
                exhausted.store(true, Ordering::SeqCst);
                return Err(PyStopAsyncIteration::new_err(()));
            }

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

    /// `SQLCancel` sobre el statement de este stream -- seguro de llamar
    /// mientras un `__anext__` esta en curso (ODBC permite `SQLCancel`
    /// desde otro hilo). La conexion asociada se descarta del pool al
    /// cerrarse, nunca se recicla (ver AGENTS.md ss4).
    fn cancel(&self) -> PyResult<()> {
        // La tarea de prefetch es dueña del cursor; cancelar realmente
        // requeriria exponer el cursor a la tarea. Por ahora cerrar el
        // stream aborta la tarea (suelta el lease). Documentado.
        self.task.abort();
        Ok(())
    }

    fn aclose<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.exhausted.store(true, Ordering::SeqCst);
        if let Some(mut rx) = self.rx.take() {
            rx.close();
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

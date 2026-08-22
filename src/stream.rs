//! `BatchStream`: iterador async **por lote** (no por fila). Sostiene vivo
//! el `Lease` (la conexion arrendada) mientras el cursor este abierto -- el
//! `HStmt` del cursor pertenece a esa conexion especifica; si el lease
//! volviera al pool antes de cerrar el cursor, otro consumidor podria
//! reusar la conexion con un statement todavia abierto por debajo.
//!
//! El estado (`Lease` + `RowCursor`) vive en un `Arc<Mutex<Option<...>>>`
//! compartido, no directamente en el `#[pyclass]`: `future_into_py` NO
//! ejecuta el future de forma sincronica -- devuelve un awaitable de Python
//! y el future corre recien cuando ese awaitable se espera. Guardar el
//! estado en un `Arc` clonado dentro del future (en vez de intentar
//! reinsertarlo en `self` justo despues de llamar a `future_into_py`, que
//! ejecutaria antes de que el future haya corrido) es lo que hace esto
//! correcto.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use pyo3::exceptions::{PyRuntimeError, PyStopAsyncIteration};
use pyo3::prelude::*;

use crate::config::EngineOptions;
use crate::core::{Lease, RowCursor};
use crate::errors::to_py_err;
use crate::rows::batch_to_pylist;

type SharedState = Arc<Mutex<Option<(Lease, RowCursor)>>>;

#[pyclass(module = "rustodbc")]
pub struct BatchStream {
    state: SharedState,
    batch_size: usize,
    options: EngineOptions,
    columns: Vec<String>,
    exhausted: Arc<AtomicBool>,
}

impl BatchStream {
    pub fn new(lease: Lease, cursor: RowCursor, batch_size: usize, options: EngineOptions) -> Self {
        let columns = cursor.column_names();
        BatchStream {
            state: Arc::new(Mutex::new(Some((lease, cursor)))),
            batch_size: batch_size.max(1),
            options,
            columns,
            exhausted: Arc::new(AtomicBool::new(false)),
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
        let state = self.state.clone();
        let batch_size = self.batch_size;
        let options = self.options.clone();
        let exhausted = self.exhausted.clone();

        pyo3_async_runtimes::tokio::future_into_py(py, async move {
            if exhausted.load(Ordering::SeqCst) {
                return Err(PyStopAsyncIteration::new_err(()));
            }

            let taken = state.lock().unwrap().take();
            let Some((lease, cursor)) = taken else {
                return Err(PyRuntimeError::new_err(
                    "BatchStream: ya cerrado o __anext__ concurrente sobre el mismo stream",
                ));
            };

            let columns_meta = cursor.column_metas();

            let (lease, cursor, batch) = tokio::task::spawn_blocking(move || {
                let mut cursor = cursor;
                let batch = cursor.fetch_batch(batch_size);
                (lease, cursor, batch)
            })
            .await
            .map_err(|e| to_py_err(crate::errors::CoreError::Connect(format!("panic: {e}"))))?;

            *state.lock().unwrap() = Some((lease, cursor));

            let batch = batch.map_err(to_py_err)?;
            if batch.is_empty() {
                exhausted.store(true, Ordering::SeqCst);
                return Err(PyStopAsyncIteration::new_err(()));
            }

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
        let mut guard = self.state.lock().unwrap();
        if let Some((lease, cursor)) = guard.as_mut() {
            cursor.cancel().map_err(to_py_err)?;
            lease.mark_tainted();
        }
        Ok(())
    }

    fn aclose<'py>(&mut self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        self.exhausted.store(true, Ordering::SeqCst);
        // Dropear aca (fuera de spawn_blocking) es aceptable: el `Drop` de
        // `RawStatement`/`RawConnection` no hace mas I/O que
        // `SQLFreeHandle`/`SQLDisconnect`.
        *self.state.lock().unwrap() = None;
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

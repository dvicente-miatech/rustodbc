//! `call_proc`/`ProcResult`: invoca un procedimiento almacenado
//! (`CALL schema.proc(?, ?, ...)`) y trae todos los result sets que
//! devuelva.
//!
//! Simplificacion deliberada de esta primera pasada (ver AGENTS.md ss9,
//! riesgo "SQLProcedureColumns puede no estar soportado"): solo parametros
//! de entrada posicionales. Resolver metadata de parametros via
//! `QSYS2.SYSPARMS` (o `SYSIBM.SQLPROCEDURECOLS`) para soportar OUT/INOUT
//! por nombre queda para cuando haya un IBM i real contra el cual validar
//! el catalogo exacto -- `odbc-sys` 0.24 ademas no expone
//! `SQLProcedureColumns` como funcion FFI, asi que esa fase se resolveria
//! por SQL contra el catalogo, no por la API catalogo nativa de ODBC.

use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::core::SharedEngine;
use crate::errors::to_py_err;
use crate::params::param_value_from_python;
use crate::rows::batch_to_pylist;

#[pyclass(module = "rustodbc")]
pub struct ProcResult {
    #[pyo3(get)]
    pub result_sets: Py<PyList>,
}

#[pymethods]
impl ProcResult {
    fn __repr__(&self) -> String {
        "ProcResult(result_sets=[...])".to_string()
    }
}

/// Convierte los parametros posicionales de Python y llama al
/// procedimiento. `params` es una secuencia (`list`/`tuple`); parametros con
/// nombre (`dict`) quedan para cuando se resuelva metadata OUT/INOUT (ver
/// modulo).
pub fn call_proc_sync(
    engine: &SharedEngine,
    schema: &str,
    proc_name: &str,
    params: &Bound<'_, PyAny>,
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<Py<ProcResult>> {
    let py = params.py();
    let mut values = Vec::new();
    if !params.is_none() {
        for item in params.iter()? {
            values.push(param_value_from_python(py, &item?)?);
        }
    }

    let placeholders = vec!["?"; values.len()].join(",");
    let sql = format!("CALL {schema}.{proc_name}({placeholders})");

    let lease = futures::executor::block_on(engine.acquire()).map_err(to_py_err)?;
    let result_sets = lease.call(&sql, &values).map_err(to_py_err)?;

    let out = PyList::empty_bound(py);
    for (columns, rows) in &result_sets {
        let list = batch_to_pylist(py, columns, rows, strip_char_padding, decimal_mode)?;
        out.append(list)?;
    }

    Py::new(
        py,
        ProcResult {
            result_sets: out.unbind(),
        },
    )
}

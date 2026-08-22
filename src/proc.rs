//! `call_proc`/`ProcResult`: invoca un procedimiento almacenado con
//! parametros por NOMBRE y trae tanto los result sets como los OUT/INOUT.
//!
//! Fiel al `call_proc` del fork C++ (`dvicente-miatech/pyodbc`,
//! `cursor.cpp::Cursor_CallProcedure`):
//! - `params` es un **dict** `{nombre_parametro: valor}`. Los parametros OUT
//!   **no necesitan venir** en el dict -- se bindean como NULL de entrada y
//!   el procedimiento igual se ejecuta; el resultado sale en `out_params`.
//! - La metadata (nombre, tipo IN/INOUT/OUT, tamano) se lee de
//!   `SQLProcedureColumns`, en orden ordinal.
//! - Se devuelve `ProcResult { result_sets, out_params }`:
//!   `result_sets` es `list[list[Row]]` y `out_params` un `dict {nombre: valor}`
//!   con los OUT/INOUT (convertidos segun el SQL type de la metadata).
//!
//! Como el C++: soporta sobrecargas solo por el primer resultado del
//! catalogo (no desambigua por `SPECIFIC_NAME`); si el procedimiento no
//! existe o no tiene parametros, `proc_columns` devuelve vacio y se falla con
//! `ParameterError` claro antes de llamar.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

use crate::core::ffi::ColumnMeta;
use crate::core::{ColumnValue, ParamValue, SharedEngine};
use crate::errors::{to_py_err, CoreError};
use crate::params::param_value_from_python;
use crate::rows::{batch_to_pylist, column_value_to_py};

#[pyclass(module = "rustodbc")]
pub struct ProcResult {
    #[pyo3(get)]
    pub result_sets: Py<PyList>,
    #[pyo3(get)]
    pub out_params: Py<PyDict>,
}

#[pymethods]
impl ProcResult {
    fn __repr__(&self) -> String {
        "ProcResult(result_sets=[...], out_params={...})".to_string()
    }
}

/// Busca `name` en el dict de entrada (`params` puede ser `None`).
/// Devuelve `None` si no esta.
fn lookup_param(
    py: Python<'_>,
    params: Option<&Bound<'_, PyDict>>,
    name: &str,
) -> PyResult<Option<ParamValue>> {
    let Some(dict) = params else {
        return Ok(None);
    };
    match dict.get_item(name)? {
        Some(value) => Ok(Some(param_value_from_python(py, &value)?)),
        None => Ok(None),
    }
}

/// Convierte los parametros posicionales de Python (dict por nombre) y llama
/// al procedimiento con bindeo OUT/INOUT. `params` es un dict o `None`;
/// acepta tambien el prefijo `@` en el nombre del dict (como el C++).
pub fn call_proc_sync(
    engine: &SharedEngine,
    schema: &str,
    proc_name: &str,
    params: &Bound<'_, PyAny>,
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<Py<ProcResult>> {
    let py = params.py();

    let input: Option<Bound<'_, PyDict>> = if params.is_none() {
        None
    } else {
        let dict = params.downcast::<PyDict>().map_err(|_| {
            to_py_err(CoreError::Parameter(
                "call_proc: params debe ser un dict {nombre: valor} (o None)".to_string(),
            ))
        })?;
        Some(dict.clone())
    };

    let lease = futures::executor::block_on(engine.acquire()).map_err(to_py_err)?;

    // 1. Metadata del procedimiento (nombres + tipo IN/OUT + SQL type).
    let metadata = lease.proc_columns(schema, proc_name).map_err(to_py_err)?;
    if metadata.is_empty() {
        return Err(to_py_err(CoreError::Parameter(format!(
            "call_proc: no se encontro el procedimiento {schema}.{proc_name} en el catalogo \
             (o no tiene parametros)"
        ))));
    }

    // 2. Resolver valores de entrada por nombre, en orden ordinal. Los OUT no
    //    necesitan venir en el dict -- quedan como entrada None y aun asi se
    //    bindean (el driver escribe el resultado).
    let mut values: Vec<Option<ParamValue>> = Vec::with_capacity(metadata.len());
    for p in &metadata {
        let mut value = lookup_param(py, input.as_ref(), &p.name)?;
        if value.is_none() && p.name.starts_with('@') {
            value = lookup_param(py, input.as_ref(), &p.name[1..])?;
        }
        values.push(value);
    }

    // 3. Ejecutar: result sets + OUT/INOUT leidos de los buffers.
    let (result_sets, out_params) = lease
        .call_proc(schema, proc_name, &metadata, &values)
        .map_err(to_py_err)?;

    // 4. Result sets -> list[list[Row]].
    let out_list = PyList::empty_bound(py);
    for (columns, rows) in &result_sets {
        let list = batch_to_pylist(py, columns, rows, strip_char_padding, decimal_mode)?;
        out_list.append(list.into_py(py))?;
    }

    // 5. OUT/INOUT -> dict {nombre: valor}, convertidos por su SQL type.
    let out_dict = PyDict::new_bound(py);
    for (idx, text) in &out_params {
        let p = &metadata[*idx];
        let meta = ColumnMeta {
            name: p.name.clone(),
            sql_type: p.sql_type,
            column_size: p.column_size,
            decimal_digits: 0,
            nullable: true,
        };
        let value = match text {
            Some(s) => column_value_to_py(
                py,
                &meta,
                &ColumnValue::Text(s.clone()),
                strip_char_padding,
                decimal_mode,
            )?,
            None => py.None(),
        };
        out_dict.set_item(&p.name, value)?;
    }

    Py::new(
        py,
        ProcResult {
            result_sets: out_list.unbind(),
            out_params: out_dict.unbind(),
        },
    )
}

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
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::core::ffi::stmt::SQL_PARAM_OUTPUT;
use crate::core::ffi::ColumnMeta;
use crate::core::{validate_proc_param, ColumnValue, ParamValue, ProcParamError, SharedEngine};
use crate::errors::{to_py_err, CoreError, ProcValidationError};
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
            decimal_digits: p.decimal_digits,
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

/// Mensaje agregado de `ProcValidationError`: lista cada variable invalida con
/// su posicion, nombre, tipo esperado y motivo. Pasa por `scrub_password` antes
/// de llegar a Python (regla dura de AGENTS.md ss8).
fn proc_validation_error(
    py: Python<'_>,
    schema: &str,
    proc_name: &str,
    failures: &[ProcParamError],
) -> PyErr {
    let parts: Vec<String> = failures
        .iter()
        .map(|f| {
            format!(
                "[{}] {} ({}): {}",
                f.index + 1,
                f.name,
                f.expected,
                f.message
            )
        })
        .collect();
    let msg = format!(
        "call_proc_args: {} parametro(s) invalido(s) para {}.{}: {}",
        failures.len(),
        schema,
        proc_name,
        parts.join("; ")
    );
    ProcValidationError::new_err(crate::errors::scrub_password(&msg))
}

/// Variante posicional de `call_proc` (`call_proc_args`): NO pide nombres, toma
/// una secuencia `list`/`tuple` en el mismo orden ordinal que devuelve
/// `SQLProcedureColumns` (IN/INOUT/OUT juntos; los OUT pueden ir como `None`).
///
/// Antes de llamar valida cada valor contra la metadata del catalogo
/// (`validate_proc_param`): largo en los tipos de caracter, parseabilidad
/// numerica, bit/fecha. Si algun valor no cumple, levanta `ProcValidationError`
/// (subclase de `ParameterError`) con el mensaje agregado de todas las
/// variables invalidas -- no corta al primero. Los errores internos del
/// procedimiento siguen saliendo como `QueryError` normal.
///
/// El bindeo sigue siendo texto (`SQL_C_WCHAR`/`VARCHAR`, ver
/// `core::bind_proc_params`): DB2 hace el cast al tipo declarado. La validacion
/// es client-side y corre ANTES de la llamada, para dar errores claros en vez
/// del truncamiento silencioso que por ejemplo cortaria `"ABC"` en un
/// `VARCHAR(1)` a `"A"`.
pub fn call_proc_args_sync(
    engine: &SharedEngine,
    schema: &str,
    proc_name: &str,
    params: &Bound<'_, PyAny>,
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<Py<ProcResult>> {
    let py = params.py();

    let lease = futures::executor::block_on(engine.acquire()).map_err(to_py_err)?;

    // 1. Metadata del procedimiento (mismo orden ordinal que la tupla).
    let metadata = lease.proc_columns(schema, proc_name).map_err(to_py_err)?;
    if metadata.is_empty() {
        return Err(to_py_err(CoreError::Parameter(format!(
            "call_proc_args: no se encontro el procedimiento {schema}.{proc_name} en el catalogo \
             (o no tiene parametros)"
        ))));
    }

    // 2. Colectar la secuencia posicional (list/tuple) o vacia si `params` es
    //    None. Un dict (call_proc por nombre) o cualquier otra cosa se rechaza
    //    con un mensaje claro.
    let items: Vec<Bound<'_, PyAny>> = if params.is_none() {
        Vec::new()
    } else if let Ok(list) = params.downcast::<PyList>() {
        list.iter().collect()
    } else if let Ok(tuple) = params.downcast::<PyTuple>() {
        tuple.iter().collect()
    } else {
        return Err(to_py_err(CoreError::Parameter(
            "call_proc_args: params debe ser una secuencia posicional (list/tuple) u None; \
             use call_proc con un dict si quiere pasar parametros por nombre"
                .to_string(),
        )));
    };

    if items.len() != metadata.len() {
        return Err(to_py_err(CoreError::Parameter(format!(
            "call_proc_args: {schema}.{proc_name} espera {} parametro(s) (en orden ordinal), \
             se recibieron {}",
            metadata.len(),
            items.len()
        ))));
    }

    // 3. Resolver valores por posicion y validar cada IN/INOUT. Se juntan TODOS
    //    los fallos antes de fallar (no corta al primero).
    let mut values: Vec<Option<ParamValue>> = Vec::with_capacity(metadata.len());
    let mut failures: Vec<ProcParamError> = Vec::new();
    for (i, param) in metadata.iter().enumerate() {
        let item = &items[i];
        if item.is_none() {
            values.push(None);
            continue;
        }
        let pv = param_value_from_python(py, item)?;
        if param.io_type == SQL_PARAM_OUTPUT {
            // OUT puro: el valor de entrada se ignora (el driver escribe el
            // resultado) -- no se valida.
            values.push(None);
        } else {
            if let Err(e) = validate_proc_param(i, param, Some(&pv)) {
                failures.push(e);
            }
            values.push(Some(pv));
        }
    }

    if !failures.is_empty() {
        return Err(proc_validation_error(py, schema, proc_name, &failures));
    }

    // 4. Ejecutar: result sets + OUT/INOUT leidos de los buffers.
    let (result_sets, out_params) = lease
        .call_proc(schema, proc_name, &metadata, &values)
        .map_err(to_py_err)?;

    // 5. Result sets -> list[list[Row]].
    let out_list = PyList::empty_bound(py);
    for (columns, rows) in &result_sets {
        let list = batch_to_pylist(py, columns, rows, strip_char_padding, decimal_mode)?;
        out_list.append(list.into_py(py))?;
    }

    // 6. OUT/INOUT -> dict {nombre: valor}, convertidos por su SQL type.
    let out_dict = PyDict::new_bound(py);
    for (idx, text) in &out_params {
        let p = &metadata[*idx];
        let meta = ColumnMeta {
            name: p.name.clone(),
            sql_type: p.sql_type,
            column_size: p.column_size,
            decimal_digits: p.decimal_digits,
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

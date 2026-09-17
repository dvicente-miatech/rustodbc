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
//!
//! Memoria (palanca 1): el drenado NO materializa todos los result sets en
//! Rust primero. Se usa `Lease::call_proc_cursor` y se trae por lotes
//! (`PROC_FETCH_CHUNK` filas): cada lote se convierte a Python y el buffer
//! Rust se libera antes del siguiente fetch. Pico ~= Python(todo) + 1 lote,
//! en vez de Rust(todo) + Python(todo). El contrato (`ProcResult` con todo)
//! no cambia -- solo el pico. Quien pueda procesar por lotes y descartar
//! usa `call_proc_stream`/`call_proc_args_stream` (`proc_stream.rs`), donde
//! la RAM no crece con el tamano.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::core::ffi::stmt::SQL_PARAM_OUTPUT;
use crate::core::ffi::ColumnMeta;
use crate::core::{
    validate_proc_param, ColumnValue, ParamValue, ProcCursor, ProcOutParams, ProcParam,
    SharedEngine,
};
use crate::errors::{to_py_err, CoreError, ProcValidationError};
use crate::params::param_value_from_python;
use crate::rows::{batch_to_pylist, column_value_to_py};

/// Filas por lote al drenar un `CALL` en el camino sync (`ProcResult`).
/// Solo acota el buffer Rust transitorio -- el resultado Python final igual
/// contiene todo (ver doc del modulo). Suficientemente grande para no
/// agregar viajes ODBC de mas, suficientemente chico para que el pico extra
/// sea despreciable frente al resultado.
pub(crate) const PROC_FETCH_CHUNK: usize = 5000;

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

/// Resuelve los valores de entrada por NOMBRE en orden ordinal (dict ya
/// validado). Los OUT que no vengan quedan como `None` -- igual se bindean
/// (el driver escribe el resultado). Puro con GIL (convierte valores Python).
pub(crate) fn resolve_named_values(
    py: Python<'_>,
    input: Option<&Bound<'_, PyDict>>,
    metadata: &[ProcParam],
) -> PyResult<Vec<Option<ParamValue>>> {
    let mut values: Vec<Option<ParamValue>> = Vec::with_capacity(metadata.len());
    for p in metadata {
        let mut value = lookup_param(py, input, &p.name)?;
        if value.is_none() && p.name.starts_with('@') {
            value = lookup_param(py, input, &p.name[1..])?;
        }
        values.push(value);
    }
    Ok(values)
}

/// Extrae los items posicionales de `params` (`list`/`tuple`) o vacio si es
/// `None`. Un `dict` u otra cosa se rechaza con `ParameterError`. Puro con
/// GIL: los llamadores lo corren ANTES de adquirir la conexion, para que un
/// `params` mal formado falle sin tocar la base (paridad con el orden de
/// errores original de `call_proc_args`).
pub(crate) fn positional_items<'py>(
    params: &Bound<'py, PyAny>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    if params.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(list) = params.downcast::<PyList>() {
        return Ok(list.iter().collect());
    }
    if let Ok(tuple) = params.downcast::<PyTuple>() {
        return Ok(tuple.iter().collect());
    }
    Err(to_py_err(CoreError::Parameter(
        "call_proc_args: params debe ser una secuencia posicional (list/tuple) u None; \
         use call_proc con un dict si quiere pasar parametros por nombre"
            .to_string(),
    )))
}

/// Valida aridad + cada valor posicional (`items` de `positional_items`)
/// contra la metadata del catalogo. Junta TODOS los fallos antes de fallar
/// (no corta al primero). Puro con GIL.
pub(crate) fn resolve_positional_items(
    py: Python<'_>,
    items: &[Bound<'_, PyAny>],
    metadata: &[ProcParam],
    schema: &str,
    proc_name: &str,
) -> PyResult<Vec<Option<ParamValue>>> {
    if items.len() != metadata.len() {
        return Err(to_py_err(CoreError::Parameter(format!(
            "call_proc_args: {schema}.{proc_name} espera {} parametro(s) (en orden ordinal), \
             se recibieron {}",
            metadata.len(),
            items.len()
        ))));
    }

    let mut values: Vec<Option<ParamValue>> = Vec::with_capacity(metadata.len());
    let mut failures: Vec<crate::core::ProcParamError> = Vec::new();
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
        return Err(proc_validation_error(schema, proc_name, &failures));
    }
    Ok(values)
}

/// `positional_items` + `resolve_positional_items` en un solo paso, para los
/// llamadores que ya tienen la metadata y el GIL (streams, blocking).
pub(crate) fn resolve_positional_values(
    py: Python<'_>,
    params: &Bound<'_, PyAny>,
    metadata: &[ProcParam],
    schema: &str,
    proc_name: &str,
) -> PyResult<Vec<Option<ParamValue>>> {
    let items = positional_items(params)?;
    resolve_positional_items(py, &items, metadata, schema, proc_name)
}

/// OUT/INOUT (`(indice, texto_o_NULL)`) -> `dict {nombre: valor}` convertido
/// por SQL type. Puro con GIL.
pub(crate) fn out_params_to_pydict(
    py: Python<'_>,
    metadata: &[ProcParam],
    out_params: &ProcOutParams,
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<Py<PyDict>> {
    let out_dict = PyDict::new_bound(py);
    for (idx, text) in out_params {
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
    Ok(out_dict.unbind())
}

/// Drena un `ProcCursor` set por set, lote por lote, convirtiendo cada lote
/// a Python y liberando el buffer Rust antes del siguiente fetch. El fetch y
/// el `advance` corren con el GIL liberado; la conversion con GIL.
///
/// Devuelve `(list[list[Row]], out_params_rust)`.
pub(crate) fn drain_proc_cursor_to_pylists(
    py: Python<'_>,
    cursor: &mut ProcCursor,
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<(Py<PyList>, ProcOutParams)> {
    let out_list = PyList::empty_bound(py);
    loop {
        if cursor.is_done() {
            break;
        }
        let columns = cursor.current_columns().to_vec();
        if columns.is_empty() {
            let has_more = py.allow_threads(|| cursor.advance()).map_err(to_py_err)?;
            if !has_more {
                break;
            }
            continue;
        }
        let set_list = PyList::empty_bound(py);
        loop {
            let batch = py
                .allow_threads(|| cursor.fetch_batch(PROC_FETCH_CHUNK))
                .map_err(to_py_err)?;
            if batch.is_empty() {
                break;
            }
            let pylist = batch_to_pylist(py, &columns, &batch, strip_char_padding, decimal_mode)?;
            for item in pylist.bind(py).iter() {
                set_list.append(item)?;
            }
            // `batch` se dropea aca: pico Rust acotado a un lote.
        }
        out_list.append(set_list)?;
        let has_more = py.allow_threads(|| cursor.advance()).map_err(to_py_err)?;
        if !has_more {
            break;
        }
    }
    let out_params = cursor.take_out_params().unwrap_or_default();
    Ok((out_list.unbind(), out_params))
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

    // El dict se retiene como handle propio: el acquire + catalogo corren con
    // el GIL liberado y el Bound original no puede usarse ahi dentro.
    let input_owned: Option<Py<PyDict>> = if params.is_none() {
        None
    } else {
        let dict = params.downcast::<PyDict>().map_err(|_| {
            to_py_err(CoreError::Parameter(
                "call_proc: params debe ser un dict {nombre: valor} (o None)".to_string(),
            ))
        })?;
        Some(dict.clone().unbind())
    };

    // 1. Lease + metadata, con el GIL liberado (ODBC bloqueante).
    let (lease, metadata) = py.allow_threads(|| {
        let lease = futures::executor::block_on(engine.acquire()).map_err(to_py_err)?;
        let metadata = lease.proc_columns(schema, proc_name).map_err(to_py_err)?;
        Ok::<_, PyErr>((lease, metadata))
    })?;
    if metadata.is_empty() {
        return Err(to_py_err(CoreError::Parameter(format!(
            "call_proc: no se encontro el procedimiento {schema}.{proc_name} en el catalogo \
             (o no tiene parametros)"
        ))));
    }

    // 2. Resolver valores por nombre (con GIL).
    let input_bound: Option<Bound<'_, PyDict>> = input_owned.as_ref().map(|d| d.bind(py).clone());
    let values = resolve_named_values(py, input_bound.as_ref(), &metadata)?;

    // 3. Cursor del CALL, con el GIL liberado (bind + exec bloqueantes).
    let mut cursor = py.allow_threads(|| {
        lease
            .call_proc_cursor(schema, proc_name, &metadata, &values)
            .map_err(to_py_err)
    })?;

    // 4. Drenar por lotes (fetch sin GIL, conversion con GIL).
    let (out_list, out_params) =
        drain_proc_cursor_to_pylists(py, &mut cursor, strip_char_padding, decimal_mode)?;

    // 5. OUT/INOUT -> dict.
    let out_dict =
        out_params_to_pydict(py, &metadata, &out_params, strip_char_padding, decimal_mode)?;

    Py::new(
        py,
        ProcResult {
            result_sets: out_list,
            out_params: out_dict,
        },
    )
}

/// Mensaje agregado de `ProcValidationError`: lista cada variable invalida con
/// su posicion, nombre, tipo esperado y motivo. Pasa por `scrub_password` antes
/// de llegar a Python (regla dura de AGENTS.md ss8).
fn proc_validation_error(
    schema: &str,
    proc_name: &str,
    failures: &[crate::core::ProcParamError],
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

    // La forma de `params` se valida ANTES de adquirir la conexion (paridad
    // con el orden de errores original): un dict u otra cosa no es secuencia.
    let items = positional_items(params)?;

    // 1. Lease + metadata, con el GIL liberado.
    let (lease, metadata) = py.allow_threads(|| {
        let lease = futures::executor::block_on(engine.acquire()).map_err(to_py_err)?;
        let metadata = lease.proc_columns(schema, proc_name).map_err(to_py_err)?;
        Ok::<_, PyErr>((lease, metadata))
    })?;
    if metadata.is_empty() {
        return Err(to_py_err(CoreError::Parameter(format!(
            "call_proc_args: no se encontro el procedimiento {schema}.{proc_name} en el catalogo \
             (o no tiene parametros)"
        ))));
    }

    // 2-3. Validar aridad + tipos (todos los fallos juntos).
    let values = resolve_positional_items(py, &items, &metadata, schema, proc_name)?;

    // 4. Cursor del CALL, con el GIL liberado.
    let mut cursor = py.allow_threads(|| {
        lease
            .call_proc_cursor(schema, proc_name, &metadata, &values)
            .map_err(to_py_err)
    })?;

    // 5. Drenar por lotes.
    let (out_list, out_params) =
        drain_proc_cursor_to_pylists(py, &mut cursor, strip_char_padding, decimal_mode)?;

    // 6. OUT/INOUT -> dict.
    let out_dict =
        out_params_to_pydict(py, &metadata, &out_params, strip_char_padding, decimal_mode)?;

    Py::new(
        py,
        ProcResult {
            result_sets: out_list,
            out_params: out_dict,
        },
    )
}

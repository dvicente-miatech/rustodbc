//! Conversion de parametros Python -> `core::ParamValue`. Todo lo que sabe
//! de PyO3 vive aca; `core::bind_params` (privado a `core`) hace el bind
//! ODBC real sobre valores ya puros de Rust.
//!
//! - `Decimal` de Python se serializa con `str(value)` -- el texto exacto
//!   que Python ya calculo para su propio `Decimal`, bindeado luego como
//!   `SQL_C_WCHAR` (nunca pasa por `f64`, ver AGENTS.md ss4).
//! - `date`/`time`/`datetime` se formatean al texto canonico de ODBC
//!   (`YYYY-MM-DD`, `HH:MM:SS[.ffffff]`, `YYYY-MM-DD HH:MM:SS[.ffffff]`) leyendo
//!   los atributos (`.year`, `.month`, ...) via `getattr` -- evita agregar
//!   `chrono` como dependencia solo para esto.
//! - `bytes`/`bytearray` -> `ParamValue::Bytes`. `None` -> `ParamValue::Null`.
//! - Inferencia por valor Python porque `SQLDescribeParam` puede no estar
//!   disponible o ser ambiguo en el driver IBM i Access (ver AGENTS.md,
//!   riesgo documentado) -- no se intenta usarlo en esta fase.

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBool, PyBoolMethods, PyBytes, PyDate, PyDateTime, PyTime};

use crate::core::ParamValue;
use crate::errors::{to_py_err, CoreError};

static DECIMAL_TYPE: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn decimal_type(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    let obj = DECIMAL_TYPE.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let module = py.import_bound("decimal")?;
        Ok(module.getattr("Decimal")?.unbind())
    })?;
    Ok(obj.bind(py))
}

/// Convierte una secuencia Python (`list`/`tuple`) de parametros posicionales
/// a `Vec<ParamValue>`. `None` (sin parametros) se convierte en un `Vec`
/// vacio. Mappings (parametros con nombre) no se soportan aca -- ODBC solo
/// tiene marcadores posicionales `?`; los parametros con nombre son
/// exclusivos de `call_proc` (`proc.rs`), que resuelve el orden via catalogo.
pub fn params_from_python(params: &Bound<'_, PyAny>) -> PyResult<Vec<ParamValue>> {
    if params.is_none() {
        return Ok(Vec::new());
    }

    let py = params.py();

    if let Ok(mapping_check) = params.downcast::<pyo3::types::PyDict>() {
        let _ = mapping_check;
        return Err(to_py_err(CoreError::Parameter(
            "parametros con nombre (dict) no soportados en execute/fetch* -- solo secuencias \
             posicionales (list/tuple) para los marcadores '?'. call_proc admite dict."
                .to_string(),
        )));
    }

    let seq = params.iter().map_err(|_| {
        to_py_err(CoreError::Parameter(
            "params debe ser una secuencia (list/tuple) o None".to_string(),
        ))
    })?;

    let mut out = Vec::new();
    for item in seq {
        let item = item?;
        out.push(param_value_from_python(py, &item)?);
    }
    Ok(out)
}

pub fn param_value_from_python(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<ParamValue> {
    if value.is_none() {
        return Ok(ParamValue::Null);
    }

    // bool ANTES que int -- en Python `bool` es subclase de `int`, y
    // `extract::<i64>()` sobre un `bool` funcionaria "por accidente" pero
    // perderiamos la intencion (bindear como 0/1 explicito, no como
    // cualquier entero).
    if let Ok(b) = value.downcast::<PyBool>() {
        return Ok(ParamValue::I64(if b.is_true() { 1 } else { 0 }));
    }

    if let Ok(i) = value.extract::<i64>() {
        return Ok(ParamValue::I64(i));
    }

    if let Ok(f) = value.extract::<f64>() {
        return Ok(ParamValue::F64(f));
    }

    let decimal_cls = decimal_type(py)?;
    if value.is_instance(decimal_cls)? {
        let text: String = value.str()?.extract()?;
        return Ok(ParamValue::Text(text));
    }

    if let Ok(dt) = value.downcast::<PyDateTime>() {
        return Ok(ParamValue::Text(format_datetime(dt.as_any())?));
    }
    if let Ok(d) = value.downcast::<PyDate>() {
        return Ok(ParamValue::Text(format_date(d.as_any())?));
    }
    if let Ok(t) = value.downcast::<PyTime>() {
        return Ok(ParamValue::Text(format_time(t.as_any())?));
    }

    if let Ok(b) = value.downcast::<PyBytes>() {
        return Ok(ParamValue::Bytes(b.as_bytes().to_vec()));
    }

    if let Ok(s) = value.extract::<String>() {
        return Ok(ParamValue::Text(s));
    }

    Err(PyTypeError::new_err(format!(
        "tipo de parametro no soportado: {}",
        value.get_type().name()?.extract::<String>()?
    )))
}

fn get_i64(obj: &Bound<'_, PyAny>, attr: &str) -> PyResult<i64> {
    obj.getattr(attr)?.extract()
}

fn format_date(d: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(format!(
        "{:04}-{:02}-{:02}",
        get_i64(d, "year")?,
        get_i64(d, "month")?,
        get_i64(d, "day")?
    ))
}

fn format_time(t: &Bound<'_, PyAny>) -> PyResult<String> {
    let micro = get_i64(t, "microsecond").unwrap_or(0);
    if micro == 0 {
        Ok(format!(
            "{:02}:{:02}:{:02}",
            get_i64(t, "hour")?,
            get_i64(t, "minute")?,
            get_i64(t, "second")?
        ))
    } else {
        Ok(format!(
            "{:02}:{:02}:{:02}.{:06}",
            get_i64(t, "hour")?,
            get_i64(t, "minute")?,
            get_i64(t, "second")?,
            micro
        ))
    }
}

fn format_datetime(dt: &Bound<'_, PyAny>) -> PyResult<String> {
    let micro = get_i64(dt, "microsecond").unwrap_or(0);
    let date_part = format!(
        "{:04}-{:02}-{:02}",
        get_i64(dt, "year")?,
        get_i64(dt, "month")?,
        get_i64(dt, "day")?
    );
    if micro == 0 {
        Ok(format!(
            "{} {:02}:{:02}:{:02}",
            date_part,
            get_i64(dt, "hour")?,
            get_i64(dt, "minute")?,
            get_i64(dt, "second")?
        ))
    } else {
        Ok(format!(
            "{} {:02}:{:02}:{:02}.{:06}",
            date_part,
            get_i64(dt, "hour")?,
            get_i64(dt, "minute")?,
            get_i64(dt, "second")?,
            micro
        ))
    }
}

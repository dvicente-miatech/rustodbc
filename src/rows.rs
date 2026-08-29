//! `core::ColumnValue` + `core::ffi::ColumnMeta` -> `list[dict]` de Python.
//!
//! Reglas duras (ver AGENTS.md ss4):
//! - DECIMAL/NUMERIC se parsean con `decimal.Decimal(texto)` -- el texto
//!   exacto que trajo `SQLGetData` como `SQL_C_WCHAR`, nunca pasa por
//!   `float`. El constructor de `Decimal` se cachea en un `GILOnceCell` (una
//!   sola lookup de atributo por vida del proceso, no por fila).
//! - Los nombres de columna se internan (`PyString::intern_bound`) una vez
//!   por statement; cada fila reusa esos mismos objetos clave.
//! - `CHAR`/`GRAPHIC` (familia `Text` sin ser DECIMAL/fecha/etc.) se
//!   recortan (`rstrip`) solo si `strip_char_padding` esta activo.
//!
//! Nota de version: este archivo usa deliberadamente las constructoras
//! `_bound` de PyO3 0.22 (`PyDict::new_bound`, `PyDate::new_bound`, etc.) en
//! vez de las variantes GIL-ref, que estan deprecadas en 0.22 y el CI
//! (`cargo clippy -D warnings`) las trata como error.

use pyo3::prelude::*;
use pyo3::sync::GILOnceCell;
use pyo3::types::{PyBytes, PyDict, PyList, PyString};
use pyo3::{IntoPy, PyObject};

use crate::core::ffi::stmt::{classify_sql_type, SqlTypeFamily};
use crate::core::ffi::ColumnMeta;
use crate::core::ColumnValue;
use crate::errors::{to_py_err, CoreError};

static DECIMAL_TYPE: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn decimal_ctor(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    let obj = DECIMAL_TYPE.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let module = py.import_bound("decimal")?;
        Ok(module.getattr("Decimal")?.unbind())
    })?;
    Ok(obj.bind(py))
}

/// Constructores del modulo `datetime` de Python. NO se usan los tipos
/// `PyDate`/`PyTime`/`PyDateTime` de pyo3 porque no existen en modo abi3
/// (pyo3 0.22 los gatea con `#[cfg(not(Py_LIMITED_API))]`) -- se construye
/// el objeto Python llamando al modulo directamente.
static DATETIME_MODULE: GILOnceCell<Py<PyAny>> = GILOnceCell::new();

fn datetime_module(py: Python<'_>) -> PyResult<&Bound<'_, PyAny>> {
    let obj = DATETIME_MODULE.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        let module = py.import_bound("datetime")?;
        Ok(module.into_any().unbind())
    })?;
    Ok(obj.bind(py))
}

fn make_date(py: Python<'_>, y: i32, m: u8, d: u8) -> PyResult<PyObject> {
    let dtmod = datetime_module(py)?;
    Ok(dtmod.call_method1("date", (y, m, d))?.unbind())
}

fn make_time(py: Python<'_>, h: u8, mi: u8, s: u8, micro: u32) -> PyResult<PyObject> {
    let dtmod = datetime_module(py)?;
    Ok(dtmod.call_method1("time", (h, mi, s, micro))?.unbind())
}

#[allow(clippy::too_many_arguments)]
fn make_datetime(
    py: Python<'_>,
    y: i32,
    m: u8,
    d: u8,
    h: u8,
    mi: u8,
    s: u8,
    micro: u32,
) -> PyResult<PyObject> {
    let dtmod = datetime_module(py)?;
    Ok(dtmod
        .call_method1("datetime", (y, m, d, h, mi, s, micro))?
        .unbind())
}

/// Convierte un batch de filas (`Vec<Vec<ColumnValue>>`) a `list[dict]`, con
/// los nombres de columna internados una sola vez para todo el batch.
pub fn batch_to_pylist(
    py: Python<'_>,
    columns: &[ColumnMeta],
    rows: &[Vec<ColumnValue>],
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<Py<PyList>> {
    let keys: Vec<Bound<'_, PyString>> = columns
        .iter()
        .map(|c| PyString::intern_bound(py, &c.name))
        .collect();

    let out = PyList::empty_bound(py);
    for row in rows {
        let dict = PyDict::new_bound(py);
        for (i, meta) in columns.iter().enumerate() {
            let value = column_value_to_py(py, meta, &row[i], strip_char_padding, decimal_mode)?;
            dict.set_item(&keys[i], value)?;
        }
        out.append(dict)?;
    }
    Ok(out.unbind())
}

pub fn column_value_to_py(
    py: Python<'_>,
    meta: &ColumnMeta,
    value: &ColumnValue,
    strip_char_padding: bool,
    decimal_mode: &str,
) -> PyResult<PyObject> {
    let family = classify_sql_type(meta.sql_type);

    match value {
        ColumnValue::Null => Ok(py.None()),
        ColumnValue::Binary(b) => Ok(PyBytes::new_bound(py, b).to_object(py)),
        ColumnValue::Text(text) => match family {
            SqlTypeFamily::Decimal => decimal_from_text(py, text, decimal_mode),
            SqlTypeFamily::Integer => match text.trim().parse::<i64>() {
                Ok(v) => Ok(v.into_py(py)),
                Err(_) => Ok(text.trim().into_py(py)),
            },
            SqlTypeFamily::Float => match text.trim().parse::<f64>() {
                Ok(v) => Ok(v.into_py(py)),
                Err(_) => Ok(text.trim().into_py(py)),
            },
            SqlTypeFamily::Bit => Ok((text.trim() == "1").into_py(py)),
            SqlTypeFamily::Date => parse_date(py, text),
            SqlTypeFamily::Time => parse_time(py, text),
            SqlTypeFamily::Timestamp => parse_timestamp(py, text),
            SqlTypeFamily::Text | SqlTypeFamily::Clob | SqlTypeFamily::Binary => {
                let s = if strip_char_padding {
                    text.trim_end()
                } else {
                    text.as_str()
                };
                Ok(s.into_py(py))
            }
        },
    }
}

fn decimal_from_text(py: Python<'_>, text: &str, decimal_mode: &str) -> PyResult<PyObject> {
    match decimal_mode {
        "str" => Ok(text.trim().into_py(py)),
        "float" => {
            let v: f64 = text.trim().parse().map_err(|_| {
                to_py_err(CoreError::Data(format!(
                    "no se pudo convertir {text:?} a float"
                )))
            })?;
            Ok(v.into_py(py))
        }
        _ => {
            let ctor = decimal_ctor(py)?;
            Ok(ctor.call1((text.trim(),))?.unbind())
        }
    }
}

fn parse_date(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let t = text.trim();
    let (y, m, d) = split_ymd(t)
        .ok_or_else(|| to_py_err(CoreError::Data(format!("fecha invalida del driver: {t:?}"))))?;
    make_date(py, y, m, d)
}

fn parse_time(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let t = text.trim();
    let (h, mi, s, micro) = split_hms(t)
        .ok_or_else(|| to_py_err(CoreError::Data(format!("hora invalida del driver: {t:?}"))))?;
    make_time(py, h, mi, s, micro)
}

fn parse_timestamp(py: Python<'_>, text: &str) -> PyResult<PyObject> {
    let t = text.trim();
    // DB2 for i puede devolver el separador fecha/hora como espacio o como
    // guion (`YYYY-MM-DD-HH.MM.SS.ffffff`, formato historico de mainframe) --
    // normalizamos aceptando ambos.
    let (date_part, time_part) = if let Some(idx) = t.find(' ') {
        (&t[..idx], &t[idx + 1..])
    } else if t.len() > 10 {
        (&t[..10], &t[11..])
    } else {
        (t, "")
    };

    let (y, m, d) = split_ymd(date_part).ok_or_else(|| {
        to_py_err(CoreError::Data(format!(
            "timestamp invalido del driver: {t:?}"
        )))
    })?;
    let (h, mi, s, micro) = if time_part.is_empty() {
        (0, 0, 0, 0)
    } else {
        split_hms(&time_part.replace('.', ":")).unwrap_or((0, 0, 0, 0))
    };

    make_datetime(py, y, m, d, h, mi, s, micro)
}

fn split_ymd(s: &str) -> Option<(i32, u8, u8)> {
    let parts: Vec<&str> = s.splitn(3, '-').collect();
    if parts.len() != 3 {
        return None;
    }
    Some((
        parts[0].parse().ok()?,
        parts[1].parse().ok()?,
        parts[2].parse().ok()?,
    ))
}

/// Acepta `HH:MM:SS`, `HH:MM:SS.ffffff` (los `:` ya normalizados desde
/// `.` si venian del formato historico).
fn split_hms(s: &str) -> Option<(u8, u8, u8, u32)> {
    let (main, frac) = s.split_once('.').unwrap_or((s, ""));
    let parts: Vec<&str> = main.splitn(3, ':').collect();
    if parts.len() < 3 {
        return None;
    }
    let h: u8 = parts[0].parse().ok()?;
    let mi: u8 = parts[1].parse().ok()?;
    let sec: u8 = parts[2].parse().ok()?;
    let micro: u32 = if frac.is_empty() {
        0
    } else {
        let padded = format!("{frac:0<6}");
        padded[..6].parse().ok()?
    };
    Some((h, mi, sec, micro))
}

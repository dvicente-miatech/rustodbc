//! `rustodbc-core`: la mitad del crate sin PyO3. Todo el `unsafe` de FFI a
//! ODBC vive contenido en `core::ffi` (ver AGENTS.md ss2 "las dos capas").
//! Este modulo expone los tipos puros (`ParamValue`, `ColumnValue`,
//! `Engine`, `Lease`, `RowCursor`) que la capa PyO3 (`engine.rs`,
//! `params.rs`, `rows.rs`, `stream.rs`) envuelve en `#[pyclass]`.
//!
//! Simplificacion deliberada respecto del diseno original (documentada
//! porque es una desviacion consciente, no un olvido): el fetch es fila a
//! fila via `SQLGetData` (no columnar con `SQL_ATTR_ROW_ARRAY_SIZE` +
//! `SQLBindCol` en arrays), porque implementar bind columnar "a ciegas" sin
//! poder probarlo contra un IBM i real es exactamente el tipo de riesgo que
//! AGENTS.md ss9 pide no tomar. El beneficio de batching en el borde
//! GIL/Python se preserva igual: `RowCursor::fetch_batch` acumula
//! `stream_batch_size` filas en Rust antes de cruzar a Python, que es donde
//! importa (una adquisicion de GIL por lote, no por fila -- ver AGENTS.md
//! ss4). El "Riesgo #1" de AGENTS.md (si el driver realmente soporta block
//! fetch) queda como optimizacion futura, no como bloqueador de esta fase.

pub mod ffi;

use std::sync::Arc;
use std::time::Duration;

use deadpool::managed::{self, Metrics, Pool, RecycleError, RecycleResult};
use secrecy::{ExposeSecret, SecretString};

use crate::errors::CoreError;
use ffi::stmt::{classify_sql_type, SqlTypeFamily};
use ffi::{ColumnMeta, RawConnection, RawStatement};

// ---------------------------------------------------------------------------
// ParamValue -- Python -> aca, sin que este modulo sepa nada de PyO3
// ---------------------------------------------------------------------------

/// Valor de parametro ya convertido desde Python (por `crate::params`, capa
/// PyO3) a una forma pura de Rust. `Text` cubre str, `Decimal` (ver AGENTS.md
/// ss4: SIEMPRE como texto, nunca float) y fechas/horas ya formateadas en el
/// texto canonico de ODBC (`YYYY-MM-DD`, `HH:MM:SS`, `YYYY-MM-DD
/// HH:MM:SS.ffffff`).
#[derive(Debug, Clone)]
pub enum ParamValue {
    Null,
    Text(String),
    I64(i64),
    F64(f64),
    Bytes(Vec<u8>),
}

/// Buffers que deben seguir vivos hasta que `SQLExecute` retorne --
/// `SQLBindParameter` solo registra punteros, el driver los lee recien al
/// ejecutar. Devuelto por `bind_params`; el llamador debe mantener el `Vec`
/// vivo hasta despues de `execute()`.
struct ParamBuffer {
    _text: Option<Vec<u16>>,
    _bytes: Option<Vec<u8>>,
    _i64: Option<Box<i64>>,
    _f64: Option<Box<f64>>,
    _indicator: Box<odbc_sys::Len>,
}

fn bind_params(stmt: &RawStatement, params: &[ParamValue]) -> Result<Vec<ParamBuffer>, CoreError> {
    use odbc_sys::{CDataType, ParamType, SqlDataType};

    let mut buffers = Vec::with_capacity(params.len());

    for (i, p) in params.iter().enumerate() {
        let param_no = (i + 1) as u16;
        match p {
            ParamValue::Null => {
                let mut indicator = Box::new(odbc_sys::NULL_DATA);
                stmt.bind_parameter(
                    param_no,
                    ParamType::Input,
                    CDataType::WChar,
                    SqlDataType::VARCHAR,
                    1,
                    0,
                    std::ptr::null_mut(),
                    0,
                    indicator.as_mut(),
                )?;
                buffers.push(ParamBuffer {
                    _text: None,
                    _bytes: None,
                    _i64: None,
                    _f64: None,
                    _indicator: indicator,
                });
            }
            ParamValue::Text(s) => {
                let mut units = ffi::wchar::to_utf16(s);
                let byte_len = (units.len() * std::mem::size_of::<u16>()) as odbc_sys::Len;
                let mut indicator = Box::new(byte_len);
                let ptr = units.as_mut_ptr() as odbc_sys::Pointer;
                stmt.bind_parameter(
                    param_no,
                    ParamType::Input,
                    CDataType::WChar,
                    SqlDataType::VARCHAR,
                    units.len().max(1),
                    0,
                    ptr,
                    byte_len,
                    indicator.as_mut(),
                )?;
                buffers.push(ParamBuffer {
                    _text: Some(units),
                    _bytes: None,
                    _i64: None,
                    _f64: None,
                    _indicator: indicator,
                });
            }
            ParamValue::I64(v) => {
                let mut boxed = Box::new(*v);
                let byte_len = std::mem::size_of::<i64>() as odbc_sys::Len;
                let mut indicator = Box::new(byte_len);
                let ptr = boxed.as_mut() as *mut i64 as odbc_sys::Pointer;
                stmt.bind_parameter(
                    param_no,
                    ParamType::Input,
                    CDataType::SBigInt,
                    SqlDataType::EXT_BIG_INT,
                    20,
                    0,
                    ptr,
                    byte_len,
                    indicator.as_mut(),
                )?;
                buffers.push(ParamBuffer {
                    _text: None,
                    _bytes: None,
                    _i64: Some(boxed),
                    _f64: None,
                    _indicator: indicator,
                });
            }
            ParamValue::F64(v) => {
                let mut boxed = Box::new(*v);
                let byte_len = std::mem::size_of::<f64>() as odbc_sys::Len;
                let mut indicator = Box::new(byte_len);
                let ptr = boxed.as_mut() as *mut f64 as odbc_sys::Pointer;
                stmt.bind_parameter(
                    param_no,
                    ParamType::Input,
                    CDataType::Double,
                    SqlDataType::DOUBLE,
                    15,
                    0,
                    ptr,
                    byte_len,
                    indicator.as_mut(),
                )?;
                buffers.push(ParamBuffer {
                    _text: None,
                    _bytes: None,
                    _i64: None,
                    _f64: Some(boxed),
                    _indicator: indicator,
                });
            }
            ParamValue::Bytes(b) => {
                let mut owned = b.clone();
                let byte_len = owned.len() as odbc_sys::Len;
                let mut indicator = Box::new(byte_len);
                let ptr = owned.as_mut_ptr() as odbc_sys::Pointer;
                stmt.bind_parameter(
                    param_no,
                    ParamType::Input,
                    CDataType::Binary,
                    SqlDataType::EXT_VAR_BINARY,
                    owned.len().max(1),
                    0,
                    ptr,
                    byte_len,
                    indicator.as_mut(),
                )?;
                buffers.push(ParamBuffer {
                    _text: None,
                    _bytes: Some(owned),
                    _i64: None,
                    _f64: None,
                    _indicator: indicator,
                });
            }
        }
    }

    Ok(buffers)
}

// ---------------------------------------------------------------------------
// call_proc -- metadata de parametros + bindeo con OUT/INOUT
// ---------------------------------------------------------------------------

/// Metadata de un parametro de procedimiento, leida de `SQLProcedureColumns`.
/// `io_type` es `SQL_PARAM_INPUT`(1)/`SQL_PARAM_INPUT_OUTPUT`(2)/`SQL_PARAM_OUTPUT`(4)
/// (ver `ffi::stmt::SQL_PARAM_*`).
#[derive(Debug, Clone)]
pub struct ProcParam {
    pub name: String,
    pub io_type: i16,
    pub sql_type: i16,
    pub column_size: usize,
    /// Tipo tal como lo reporta el catalogo (`TYPE_NAME` de
    /// `SQLProcedureColumns`, columna 7), p.ej. `VARCHAR`, `DECIMAL`, `DATE`.
    /// Vacio si el driver no lo reporta.
    pub type_name: String,
    /// Escala del tipo (columna `DECIMAL_DIGITS`, 10). Relevante para
    /// `DECIMAL(p,s)` en mensajes de error de validacion.
    pub decimal_digits: i16,
}

/// Buffer de un parametro de `CALL`. Para IN/INOUT el valor de entrada se
/// escribe en el buffer ANTES de ejecutar; para OUT/INOUT el driver escribe el
/// resultado EN el mismo buffer (o en `_out`) y deja la longitud en
/// `_indicator` despues de `SQLExecute`. `read_out()` recupera el texto.
///
/// Los LOB de entrada (`LobIn`) NO reservan buffer: se bindean con
/// `SQL_LEN_DATA_AT_EXEC` y se entregan por chunks con `SQLPutData`
/// (`feed_lob_inputs`) despues de que `SQLExecute` devuelva `SQL_NEED_DATA`.
/// El `String` completo vive en `_lob_text` hasta que termina la alimentacion.
struct ProcParamBuffer {
    _buf: Vec<u16>,
    _indicator: Box<odbc_sys::Len>,
    /// Texto de entrada de un parametro LOB (IN/INOUT de familia `Clob`).
    /// Vive hasta que `feed_lob_inputs` termina de entregar sus chunks.
    _lob_text: Option<String>,
}

impl ProcParamBuffer {
    fn read_out(&self) -> Option<String> {
        let ind = *self._indicator;
        if ind == odbc_sys::NULL_DATA {
            return None;
        }
        // `ind` viene en BYTES para SQL_C_WCHAR (como en get_data_text).
        let units = if ind < 0 {
            self._buf.len()
        } else {
            ((ind as usize) / std::mem::size_of::<u16>()).min(self._buf.len())
        };
        // Defensivo: si el driver trunco (`ind` mas largo que el buffer) o no
        // entrego longitud (`ind < 0`), el slice puede incluir el NUL de
        // SQL_C_WCHAR al final -- recortar la cola de NULs antes de decodificar.
        let end = match self._buf[..units].iter().rposition(|&u| u != 0) {
            Some(i) => i + 1,
            None => 0,
        };
        Some(ffi::wchar::from_utf16_lossy(&self._buf[..end]))
    }
}

fn param_value_to_text(v: Option<&ParamValue>) -> Option<String> {
    match v {
        None => None,
        Some(ParamValue::Null) => None,
        Some(ParamValue::Text(s)) => Some(s.clone()),
        Some(ParamValue::I64(i)) => Some(i.to_string()),
        Some(ParamValue::F64(f)) => Some(f.to_string()),
        Some(ParamValue::Bytes(b)) => Some(String::from_utf8_lossy(b).into_owned()),
    }
}

/// Error de validacion de un parametro posicional de `CALL`, listo para
/// mostrarse en el mensaje agregado de `ProcValidationError`. `index` es la
/// posicion 0-based en la tupla recibida.
#[derive(Debug, Clone)]
pub struct ProcParamError {
    pub index: usize,
    pub name: String,
    /// Tipo esperado legible, p.ej. `VARCHAR(1)`, `DECIMAL(5,2)`.
    pub expected: String,
    /// Valor tal como llego (texto), p.ej. `ABC`.
    pub provided: String,
    pub message: String,
}

/// Texto legible del tipo de un parametro de procedimiento, usado en mensajes
/// de error de validacion. Ejemplos: `VARCHAR(1)`, `CHAR(3)`, `DECIMAL(5,2)`,
/// `INTEGER`, `DATE`. Cae al nombre de la familia si el catalogo no reporto
/// `TYPE_NAME`.
pub fn format_param_type(p: &ProcParam) -> String {
    let base = if p.type_name.is_empty() {
        format!("{:?}", classify_sql_type(p.sql_type))
    } else {
        p.type_name.clone()
    };
    match classify_sql_type(p.sql_type) {
        SqlTypeFamily::Text | SqlTypeFamily::Clob => {
            if p.column_size > 0 {
                format!("{base}({})", p.column_size)
            } else {
                base
            }
        }
        SqlTypeFamily::Decimal => {
            if p.column_size > 0 {
                format!("{base}({}, {})", p.column_size, p.decimal_digits)
            } else {
                base
            }
        }
        _ => base,
    }
}

/// Check barato de parseabilidad numerica: acepta `123`, `-10`, `123.45`,
/// `.5`, `123.`; rechaza `""`, `"-"`, `"."`, `"ABC"`.
fn looks_numeric(text: &str) -> bool {
    let t = text.trim();
    if t.is_empty() {
        return false;
    }
    let mut has_digit = false;
    for c in t.chars() {
        match c {
            '0'..='9' => has_digit = true,
            '.' | '-' => {}
            _ => return false,
        }
    }
    has_digit
}

fn truncate_for_msg(text: &str, max: usize) -> String {
    if text.chars().count() <= max {
        text.to_string()
    } else {
        let mut out: String = text.chars().take(max).collect();
        out.push('…');
        out
    }
}

/// Valida un parametro posicional de `CALL` contra su metadata del catalogo
/// (`ProcParam`). Devuelve `Ok(())` si el valor (ya convertido a texto) cumple
/// con el tipo y el tamano declarados del parametro, o `Err(ProcParamError)` si
/// no. `None`/`Null` se aceptan sin validar (NULL es siempre un input valido).
///
/// Reglas (ver AGENTS.md ss4): los `DECIMAL`/`NUMERIC`/`DECFLOAT` viajan como
/// texto (nunca float). La validacion es client-side y corre ANTES de llamar al
/// procedimiento, para dar errores claros en vez del truncamiento silencioso
/// que hoy hace `bind_proc_params` (que corta el texto a `column_size`).
pub fn validate_proc_param(
    index: usize,
    param: &ProcParam,
    value: Option<&ParamValue>,
) -> Result<(), ProcParamError> {
    let Some(v) = value else {
        return Ok(());
    };
    if matches!(v, ParamValue::Null) {
        return Ok(());
    }
    let text = param_value_to_text(Some(v)).unwrap_or_default();
    let expected = format_param_type(param);
    let provided = text.clone();
    let family = classify_sql_type(param.sql_type);

    let err = |message: String| ProcParamError {
        index,
        name: param.name.clone(),
        expected: expected.clone(),
        provided: provided.clone(),
        message,
    };

    match family {
        // Largo en UTF-16 code units: la misma metrica que usa el bind
        // (`text.encode_utf16()`), asi validacion y bindeo coinciden.
        SqlTypeFamily::Text => {
            let len = text.encode_utf16().count();
            if len > param.column_size {
                Err(err(format!(
                    "el valor '{}' excede el largo maximo {} de {}",
                    truncate_for_msg(&provided, 32),
                    param.column_size,
                    expected
                )))
            } else {
                Ok(())
            }
        }
        // Un LOB de entrada viaja por data-at-execution (`feed_lob_inputs`): no
        // hay buffer que desbordar, el driver consume el valor por chunks. El
        // catalogo reporta `column_size` como el maximo declarado del tipo, no
        // como tope del bind -- validar contra eso rechazaria valores que el
        // driver si acepta (p.ej. un CLOB(50M) justo en el limite).
        SqlTypeFamily::Clob => Ok(()),
        SqlTypeFamily::Decimal => {
            if looks_numeric(&text) {
                Ok(())
            } else {
                Err(err(format!(
                    "'{}' no es un numero valido para {}",
                    truncate_for_msg(&provided, 32),
                    expected
                )))
            }
        }
        SqlTypeFamily::Integer => {
            if text.trim().parse::<i64>().is_ok() {
                Ok(())
            } else {
                Err(err(format!(
                    "'{}' no es un entero valido para {}",
                    truncate_for_msg(&provided, 32),
                    expected
                )))
            }
        }
        SqlTypeFamily::Float => {
            if text.trim().parse::<f64>().is_ok() {
                Ok(())
            } else {
                Err(err(format!(
                    "'{}' no es un numero valido para {}",
                    truncate_for_msg(&provided, 32),
                    expected
                )))
            }
        }
        SqlTypeFamily::Bit => {
            let t = text.trim();
            if t == "0"
                || t == "1"
                || t.eq_ignore_ascii_case("true")
                || t.eq_ignore_ascii_case("false")
            {
                Ok(())
            } else {
                Err(err(format!(
                    "'{}' no es un booleano valido para {} (0/1 o true/false)",
                    truncate_for_msg(&provided, 32),
                    expected
                )))
            }
        }
        SqlTypeFamily::Date | SqlTypeFamily::Time | SqlTypeFamily::Timestamp => {
            if text.trim().is_empty() {
                Err(err(format!("valor vacio no valido para {}", expected)))
            } else {
                Ok(())
            }
        }
        SqlTypeFamily::Binary => Ok(()),
    }
}

/// Bindeo de los parametros de un `CALL`. Igual que `bind_params`, los
/// buffers deben seguir vivos hasta despues de `execute()` -- este `Vec` es
/// quien los sostiene, y de donde `Lease::call_proc` lee los OUT despues.
///
/// Parametros de entrada de la familia `Clob` (IN/INOUT CLOB/DBCLOB/
/// LONGVARCHAR) con valor NO VACIO van por **data-at-execution**
/// (`feed_lob_inputs`, ver abajo): se bindean como `SQL_C_CHAR`/`CLOB` con
/// `SQL_LEN_DATA_AT_EXEC(len)` y se entregan por chunks de 1 MB con
/// `SQLPutData`, sin tope de tamano y sin copiar el valor completo mas de
/// una vez. Los demas parametros usan el camino historico (buffer UTF-16
/// capado a 64K, error explicito en vez de truncamiento silencioso).
fn bind_proc_params(
    stmt: &RawStatement,
    params: &[ProcParam],
    values: &[Option<ParamValue>],
) -> Result<Vec<ProcParamBuffer>, CoreError> {
    use odbc_sys::{CDataType, ParamType, SqlDataType};

    let mut buffers = Vec::with_capacity(params.len());

    for (i, p) in params.iter().enumerate() {
        let param_no = (i + 1) as u16;
        let is_out = p.io_type == ffi::stmt::SQL_PARAM_OUTPUT
            || p.io_type == ffi::stmt::SQL_PARAM_INPUT_OUTPUT;
        let is_in = p.io_type == ffi::stmt::SQL_PARAM_INPUT
            || p.io_type == ffi::stmt::SQL_PARAM_INPUT_OUTPUT;

        // Rama LOB de entrada: CLOB/DBCLOB/LONGVARCHAR con texto no vacio.
        // Data-at-execution: sin buffer de valor (el driver lo pide por
        // chunks), indicador `SQL_LEN_DATA_AT_EXEC(len_bytes)` y SQL type real
        // del catalogo (`p.sql_type`, no VARCHAR). UTF-8 (`SQL_C_CHAR`): el
        // driver IBM i Access ODBC no maneja CLOB por `SQL_C_WCHAR` (ver
        // `get_data_text_lob`).
        if is_in && classify_sql_type(p.sql_type) == SqlTypeFamily::Clob {
            if let Some(text) = param_value_to_text(values.get(i).and_then(|v| v.as_ref())) {
                if !text.is_empty() {
                    let byte_len = text.len();
                    let mut indicator = Box::new(odbc_sys::indicator::len_data_at_exec(
                        byte_len as odbc_sys::Len,
                    ));
                    stmt.bind_parameter(
                        param_no,
                        match p.io_type {
                            ffi::stmt::SQL_PARAM_INPUT_OUTPUT => ParamType::InputOutput,
                            _ => ParamType::Input,
                        },
                        CDataType::Char,
                        SqlDataType(p.sql_type),
                        p.column_size,
                        p.decimal_digits,
                        // Token que el driver devuelve en SQLParamData: el
                        // numero de parametro (1-based) como puntero. La spec
                        // manda de vuelta el ParameterValuePtr bindeado, no la
                        // direccion del indicador.
                        param_no as usize as odbc_sys::Pointer,
                        byte_len as odbc_sys::Len,
                        indicator.as_mut(),
                    )?;
                    buffers.push(ProcParamBuffer {
                        _buf: Vec::new(),
                        _indicator: indicator,
                        _lob_text: Some(text),
                    });
                    continue;
                }
            }
        }

        // El C++ (cursor.cpp) toma COLUMN_SIZE del catalogo, capa a [256, 64KB],
        // y bindea todo como texto. Aca lo mismo pero UTF-16.
        //
        // Off-by-one 0.7.0/0.7.1: el buffer se dimensionaba a `cap` u16 (=cap*2
        // bytes) sin sitio para el NUL de SQL_C_WCHAR; el driver escribia
        // cap-1 caracteres + NUL y read_out() devolvia el NUL pegado al texto
        // (un CHAR(22) perdia su ultimo caracter). El +1 deja sitio al
        // terminador; la longitud real se lee del indicador (read_out).
        let cap = p.column_size.clamp(1, 65536);
        let mut buf = vec![0u16; cap + 1];
        let mut indicator = Box::new(odbc_sys::Len::default());

        // Escribir el valor de entrada si lo hay (IN/INOUT); si no, NULL.
        // Sin truncamiento silencioso: si el texto excede el buffer se
        // devuelve error explicito (la validacion de `validate_proc_param`
        // ya lo detecta antes; esto es la red de seguridad).
        if is_in {
            if let Some(text) = param_value_to_text(values.get(i).and_then(|v| v.as_ref())) {
                let units = text.encode_utf16().collect::<Vec<_>>();
                if units.len() > cap {
                    return Err(CoreError::Parameter(format!(
                        "parametro {} ({}): el valor ({} unidades) excede el buffer de {} del bind",
                        i + 1,
                        p.name,
                        units.len(),
                        cap,
                    )));
                }
                let n = units.len();
                buf[..n].copy_from_slice(&units[..n]);
                *indicator = (n * std::mem::size_of::<u16>()) as odbc_sys::Len;
            } else {
                *indicator = odbc_sys::NULL_DATA;
            }
        } else if is_out {
            // OUT puro: el driver escribe el resultado; el indicador de entrada
            // no importa (el driver lo sobreescribe).
            *indicator = odbc_sys::NULL_DATA;
        }

        let io_type = match p.io_type {
            ffi::stmt::SQL_PARAM_INPUT_OUTPUT => ParamType::InputOutput,
            ffi::stmt::SQL_PARAM_OUTPUT => ParamType::Output,
            _ => ParamType::Input,
        };

        let byte_len = (buf.len() * std::mem::size_of::<u16>()) as odbc_sys::Len;
        let ptr = buf.as_mut_ptr() as odbc_sys::Pointer;
        stmt.bind_parameter(
            param_no,
            io_type,
            CDataType::WChar,
            SqlDataType::VARCHAR,
            cap,
            0,
            ptr,
            byte_len,
            indicator.as_mut(),
        )?;

        buffers.push(ProcParamBuffer {
            _buf: buf,
            _indicator: indicator,
            _lob_text: None,
        });
    }

    Ok(buffers)
}

/// Entrega los parametros LOB de entrada por chunks (`SQLPutData`) despues de
/// que `execute()` devuelva `SQL_NEED_DATA`.
///
/// Flujo:
/// 1. `SQLExecute` -> `SQL_NEED_DATA`: hay data-at-exec pendientes.
/// 2. Loop: `SQLParamData` devuelve el token del parametro (el numero 1-based
///    bindeado como `ParameterValuePtr`); se buscan sus bytes UTF-8 y se
///    entregan en chunks de 1 MB con `SQLPutData(ptr, len)`.
/// 3. `SQLParamData` -> `NO_DATA`: listo, el statement sigue su curso normal
///    (result sets / OUT).
///
/// El texto UTF-8 vive en `ProcParamBuffer::_lob_text` durante todo el ciclo;
/// cada `SQLPutData` recibe un slice de ese texto y lo consume de forma
/// sincronica, antes de retornar. Si el driver pide un parametro que no es LOB
/// registrado, se devuelve error en vez de inventar datos.
pub fn feed_lob_inputs(stmt: &RawStatement, buffers: &[ProcParamBuffer]) -> Result<(), CoreError> {
    const CHUNK: usize = 1024 * 1024;

    if !buffers.iter().any(|b| b._lob_text.is_some()) {
        return Ok(());
    }

    loop {
        let wanted = stmt.param_data()?;
        let Some(ptr) = wanted else {
            return Ok(()); // NO_DATA: alimentacion completa.
        };
        // El token que devuelve `SQLParamData` es el `ParameterValuePtr` que se
        // bindeo (el numero de parametro 1-based como puntero; ver
        // `bind_proc_params`). NO es la direccion del indicador.
        let idx = (ptr as usize)
            .checked_sub(1)
            .filter(|&k| k < buffers.len())
            .filter(|&k| buffers[k]._lob_text.is_some())
            .ok_or_else(|| {
                CoreError::Parameter(format!(
                    "el driver pidio data-at-execution de un parametro no registrado como LOB (token {})",
                    ptr as usize,
                ))
            })?;
        let bytes = buffers[idx]._lob_text.as_deref().unwrap_or("").as_bytes();
        // Chunks de 1 MB sin partir un caracter UTF-8 multibyte al medio: el
        // driver interpreta la secuencia de `SQLPutData` como un flujo de
        // bytes, y un corte a mitad de secuencia lo corrompe.
        let mut start = 0;
        while start < bytes.len() {
            let mut end = (start + CHUNK).min(bytes.len());
            while end < bytes.len() && (bytes[end] & 0xC0) == 0x80 {
                end -= 1;
            }
            stmt.put_data(
                bytes[start..end].as_ptr() as odbc_sys::Pointer,
                (end - start) as odbc_sys::Len,
            )?;
            start = end;
        }
        // Chunk final vacio = fin de datos de este parametro (requerido por
        // la spec: `SQLPutData` con largo 0 cierra el valor).
        stmt.put_data(std::ptr::null_mut(), 0)?;
    }
}

/// Valor de columna crudo, ya leido del driver pero sin convertir a un tipo
/// de Python. `Text` incluye numeros/decimales/fechas -- `crate::rows`
/// decide como parsearlo segun `ColumnMeta::sql_type` (via
/// `classify_sql_type`).
#[derive(Debug, Clone)]
pub enum ColumnValue {
    Null,
    Text(String),
    Binary(Vec<u8>),
}

fn fetch_row(stmt: &RawStatement, columns: &[ColumnMeta]) -> Result<Vec<ColumnValue>, CoreError> {
    let mut row = Vec::with_capacity(columns.len());
    for (i, meta) in columns.iter().enumerate() {
        let col = (i + 1) as u16;
        let value = match classify_sql_type(meta.sql_type) {
            SqlTypeFamily::Binary => match stmt.get_data_binary(col)? {
                Some(b) => ColumnValue::Binary(b),
                None => ColumnValue::Null,
            },
            SqlTypeFamily::Clob => match stmt.get_data_text(col)? {
                Some(s) => ColumnValue::Text(s),
                // El driver iSeries de Windows no entrega CLOB por SQL_C_WCHAR
                // (primera llamada sin datos; validado contra DEV con CCSID 284
                // y 1208). Fallback a SQL_C_CHAR solo para ese caso: en Linux el
                // camino WCHAR funciona y el CHAR corrompe LOBs multichunk.
                None => match stmt.get_data_text_lob(col)? {
                    Some(s) => ColumnValue::Text(s),
                    None => ColumnValue::Null,
                },
            },
            _ => match stmt.get_data_text(col)? {
                Some(s) => ColumnValue::Text(s),
                None => ColumnValue::Null,
            },
        };
        row.push(value);
    }
    Ok(row)
}

fn describe_columns(stmt: &RawStatement) -> Result<Vec<ColumnMeta>, CoreError> {
    let ncols = stmt.num_result_cols()?;
    (1..=ncols)
        .map(|i| stmt.describe_col(i as u16))
        .collect::<Result<Vec<_>, _>>()
}

// ---------------------------------------------------------------------------
// RowCursor -- statement + columnas vivas para streaming por lotes
// ---------------------------------------------------------------------------

/// Cursor de una consulta en curso. Vive mientras el `BatchStream` (capa
/// PyO3) lo consuma; se dropea (y libera el `HStmt`) cuando el stream se
/// cierra o se dropea sin cerrar (`Drop` de `RawStatement` ya libera el
/// handle, no hace falta logica extra aca).
pub struct RowCursor {
    stmt: RawStatement,
    columns: Vec<ColumnMeta>,
    exhausted: bool,
    /// Mantiene vivos los buffers de parametros mientras el cursor exista --
    /// en teoria alcanza con que sobrevivan a `execute()`, pero es mas
    /// simple sostenerlos durante toda la vida del cursor.
    _param_buffers: Vec<ParamBuffer>,
}

impl RowCursor {
    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn column_metas(&self) -> Vec<ColumnMeta> {
        self.columns.clone()
    }

    /// Trae hasta `max_rows` filas. `Vec` vacio = result set agotado.
    pub fn fetch_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<ColumnValue>>, CoreError> {
        if self.exhausted {
            return Ok(Vec::new());
        }
        let mut batch = Vec::with_capacity(max_rows);
        for _ in 0..max_rows {
            if !self.stmt.fetch()? {
                self.exhausted = true;
                break;
            }
            batch.push(fetch_row(&self.stmt, &self.columns)?);
        }
        Ok(batch)
    }

    /// `SQLCancel` sobre el statement de este cursor -- seguro de llamar
    /// desde otro hilo (ver `core::ffi::stmt`). La conexion asociada se
    /// descarta del pool despues de esto (`Lease::cancel_and_taint`).
    pub fn cancel(&self) -> Result<(), CoreError> {
        self.stmt.cancel()
    }
}

// ---------------------------------------------------------------------------
// ProcCursor -- cursor multi-result-set para CALL con OUT/INOUT
// ---------------------------------------------------------------------------

/// Cursor de un `CALL schema.proc(?,...)` en curso. A diferencia de
/// `RowCursor` (un solo result set), un procedimiento puede devolver N result
/// sets via `SQLMoreResults`, y los OUT/INOUT solo son validos despues de
/// drenar TODOS los sets (spec ODBC: `SQLMoreResults` -> `SQL_NO_DATA`).
///
/// Los `ProcParamBuffer` se sostienen vivos en `buffers` hasta que `advance()`
/// agota el ultimo set y lee los OUT a `out_params`. `set_index` cuenta solo
/// sets ENTREGADOS (con columnas, igual que `ProcResult.result_sets`): los
/// sets vacios (0 columnas) se saltan tanto al posicionar como al avanzar,
/// paridad exacta con el `Lease::call_proc` historico.
pub struct ProcCursor {
    stmt: RawStatement,
    buffers: Vec<ProcParamBuffer>,
    out_indices: Vec<usize>,
    columns: Vec<ColumnMeta>,
    set_index: usize,
    all_done: bool,
    out_params: Option<ProcOutParams>,
}

impl ProcCursor {
    /// Indice (0-based) del result set actual entre los entregados.
    pub fn set_index(&self) -> usize {
        self.set_index
    }

    /// `true` cuando ya se drenaron todos los sets y se leyeron los OUT.
    pub fn is_done(&self) -> bool {
        self.all_done
    }

    /// Metadata del result set actual (vacia si `is_done()`).
    pub fn current_columns(&self) -> &[ColumnMeta] {
        &self.columns
    }

    pub fn column_names(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.name.clone()).collect()
    }

    pub fn column_metas(&self) -> Vec<ColumnMeta> {
        self.columns.clone()
    }

    /// Trae hasta `max_rows` filas del set ACTUAL. `Vec` vacio = set actual
    /// agotado (no proc agotado: llamar `advance()` para pasar al siguiente).
    pub fn fetch_batch(&mut self, max_rows: usize) -> Result<Vec<Vec<ColumnValue>>, CoreError> {
        if self.all_done || max_rows == 0 {
            return Ok(Vec::new());
        }
        let mut batch = Vec::with_capacity(max_rows);
        for _ in 0..max_rows {
            if !self.stmt.fetch()? {
                break;
            }
            batch.push(fetch_row(&self.stmt, &self.columns)?);
        }
        Ok(batch)
    }

    /// Avanza al siguiente result set con columnas. `Ok(true)` = hay otro
    /// set (actualiza `current_columns` + `set_index`); `Ok(false)` = no hay
    /// mas (marca `all_done` y lee los OUT/INOUT de los buffers).
    pub fn advance(&mut self) -> Result<bool, CoreError> {
        if self.all_done {
            return Ok(false);
        }
        loop {
            if !self.stmt.more_results()? {
                self.finish_out();
                return Ok(false);
            }
            let columns = describe_columns(&self.stmt).unwrap_or_default();
            if columns.is_empty() {
                continue;
            }
            self.columns = columns;
            self.set_index += 1;
            return Ok(true);
        }
    }

    /// OUT/INOUT leidos al agotar (`Some` solo tras `advance()` -> `false`).
    pub fn take_out_params(&mut self) -> Option<ProcOutParams> {
        self.out_params.take()
    }

    /// `SQLCancel` sobre el statement -- seguro desde otro hilo. La conexion
    /// asociada se descarta del pool despues de esto (regla AGENTS.md ss4).
    pub fn cancel(&self) -> Result<(), CoreError> {
        self.stmt.cancel()
    }

    fn finish_out(&mut self) {
        let mut out = Vec::with_capacity(self.out_indices.len());
        for &i in &self.out_indices {
            let text = self.buffers.get(i).and_then(|b| b.read_out());
            out.push((i, text));
        }
        self.columns = Vec::new();
        self.all_done = true;
        self.out_params = Some(out);
    }
}

// ---------------------------------------------------------------------------
// Lease -- una conexion arrendada del pool
// ---------------------------------------------------------------------------

pub struct Lease {
    conn: managed::Object<ConnManager>,
}

impl Lease {
    fn hdbc(&self) -> odbc_sys::HDbc {
        self.conn.handle()
    }

    /// Marca la conexion como "tainted" (ver AGENTS.md ss4: una conexion que
    /// sostuvo `SESSION.*` o cuyo statement se cancelo se descarta, nunca se
    /// recicla). El pool la dropea en vez de reciclarla al liberarla.
    pub fn mark_tainted(&mut self) {
        self.conn.tainted = true;
    }

    /// Consume la conexion permanentemente: la saca del pool y la cierra
    /// fisica (`SQLDisconnect` + free al dropear). Usado por la cancelacion
    /// de streams -- una conexion con statement cancelado se descarta, nunca
    /// se recicla (regla dura AGENTS.md ss4).
    pub fn take_connection(self) -> ffi::RawConnection {
        managed::Object::take(self.conn)
    }

    pub fn set_autocommit(&self, on: bool) -> Result<(), CoreError> {
        self.conn.set_autocommit(on)
    }

    pub fn commit(&self) -> Result<(), CoreError> {
        end_tran(self.hdbc(), odbc_sys::CompletionType::Commit)
    }

    pub fn rollback(&self) -> Result<(), CoreError> {
        end_tran(self.hdbc(), odbc_sys::CompletionType::Rollback)
    }

    /// Ejecuta `sql` y descarta cualquier result set -- devuelve el
    /// rowcount (`SQLRowCount`). Usado por `execute()`.
    pub fn execute(&self, sql: &str, params: &[ParamValue]) -> Result<i64, CoreError> {
        let stmt = RawStatement::alloc(self.hdbc())?;
        let _buffers = bind_params(&stmt, params)?;
        match stmt.exec_direct(sql)? {
            // El driver IBM i Access ODBC devuelve SQL_NO_DATA para un
            // UPDATE/DELETE que afecta 0 filas (no es un error; ver
            // `ExecOutcome` en core::ffi::stmt). Se traduce a rowcount 0 sin
            // depender de SQLRowCount despues de un NO_DATA.
            ffi::stmt::ExecOutcome::NoData => Ok(0),
            ffi::stmt::ExecOutcome::Executed => stmt.row_count(),
        }
    }

    /// Ejecuta `sql` y trae TODAS las filas en memoria de una. Usado por
    /// `fetch_all`/`fetch_one`/`fetch_value`/`fetch_column`.
    pub fn query(
        &self,
        sql: &str,
        params: &[ParamValue],
    ) -> Result<(Vec<ColumnMeta>, Vec<Vec<ColumnValue>>), CoreError> {
        let stmt = RawStatement::alloc(self.hdbc())?;
        let _buffers = bind_params(&stmt, params)?;
        stmt.exec_direct(sql)?;
        let columns = describe_columns(&stmt)?;
        let mut rows = Vec::new();
        while stmt.fetch()? {
            rows.push(fetch_row(&stmt, &columns)?);
        }
        Ok((columns, rows))
    }

    /// Version streaming de `query`: ejecuta y devuelve un `RowCursor` que
    /// el llamador va drenando con `fetch_batch`, sin materializar el result
    /// set completo en memoria.
    pub fn query_cursor(&self, sql: &str, params: &[ParamValue]) -> Result<RowCursor, CoreError> {
        let stmt = RawStatement::alloc(self.hdbc())?;
        let buffers = bind_params(&stmt, params)?;
        stmt.exec_direct(sql)?;
        let columns = describe_columns(&stmt)?;
        Ok(RowCursor {
            stmt,
            columns,
            exhausted: false,
            _param_buffers: buffers,
        })
    }

    /// Lee la metadata de los parametros del procedimiento `schema.proc` via
    /// `SQLProcedureColumns` (nombre, tipo IN/INOUT/OUT, SQL type, tamano),
    /// en orden ordinal. Solo incluye parametros IN/INOUT/OUT (no
    /// SQL_RETURN_VALUE). Devuelve `Vec` vacio si el procedimiento no existe
    /// o no tiene parametros.
    pub fn proc_columns(&self, schema: &str, proc_name: &str) -> Result<Vec<ProcParam>, CoreError> {
        let stmt = RawStatement::alloc(self.hdbc())?;
        stmt.procedure_columns(schema, proc_name)?;

        let mut params = Vec::new();
        while stmt.fetch()? {
            // Columnas de SQLProcedureColumns: 4=COLUMN_NAME, 5=COLUMN_TYPE,
            // 6=DATA_TYPE, 7=TYPE_NAME, 8=COLUMN_SIZE, 10=DECIMAL_DIGITS.
            // Se leen como texto (SQLGetData).
            let name = stmt.get_data_text(4)?.unwrap_or_default();
            let io_type = stmt
                .get_data_text(5)?
                .and_then(|s| s.trim().parse::<i16>().ok())
                .unwrap_or(0);
            let sql_type = stmt
                .get_data_text(6)?
                .and_then(|s| s.trim().parse::<i16>().ok())
                .unwrap_or(0);
            let type_name = stmt.get_data_text(7)?.unwrap_or_default();
            let column_size = stmt
                .get_data_text(8)?
                .and_then(|s| s.trim().parse::<usize>().ok())
                .unwrap_or(0);
            let decimal_digits = stmt
                .get_data_text(10)?
                .and_then(|s| s.trim().parse::<i16>().ok())
                .unwrap_or(0);

            let is_param = io_type == ffi::stmt::SQL_PARAM_INPUT
                || io_type == ffi::stmt::SQL_PARAM_INPUT_OUTPUT
                || io_type == ffi::stmt::SQL_PARAM_OUTPUT;
            if is_param {
                params.push(ProcParam {
                    name,
                    io_type,
                    sql_type,
                    column_size,
                    type_name,
                    decimal_digits,
                });
            }
        }
        Ok(params)
    }

    /// Ejecuta un `CALL schema.proc(?,...)` y devuelve un `ProcCursor` para
    /// drenarlo por lotes sin materializar todos los result sets en memoria.
    /// Los OUT/INOUT se leen solos cuando `advance()` agota el ultimo set.
    ///
    /// `metadata` es el resultado de `proc_columns` (mismo orden); `values`
    /// son los valores de entrada por posicion (`None` = NULL de entrada; los
    /// OUT pueden ir como `None` sin problema -- el driver escribe el
    /// resultado).
    pub fn call_proc_cursor(
        &self,
        schema: &str,
        proc_name: &str,
        metadata: &[ProcParam],
        values: &[Option<ParamValue>],
    ) -> Result<ProcCursor, CoreError> {
        let placeholders = vec!["?"; metadata.len()].join(",");
        let sql = format!("{{CALL {schema}.{proc_name}({placeholders})}}");

        let stmt = RawStatement::alloc(self.hdbc())?;
        let buffers = bind_proc_params(&stmt, metadata, values)?;
        // Si hay LOBs de entrada por data-at-execution, `exec_direct` puede
        // devolver `SQL_NEED_DATA`: hay que alimentar los chunks ANTES de
        // seguir (ver `feed_lob_inputs`). Si el driver no los pidio (no
        // deberia, pero podria), no se alimenta nada.
        if buffers.iter().any(|b| b._lob_text.is_some()) {
            if stmt.exec_direct_need_data(&sql)? {
                feed_lob_inputs(&stmt, &buffers)?;
            }
        } else {
            stmt.exec_direct(&sql)?;
        }

        let out_indices: Vec<usize> = metadata
            .iter()
            .enumerate()
            .filter(|(_, p)| {
                p.io_type == ffi::stmt::SQL_PARAM_OUTPUT
                    || p.io_type == ffi::stmt::SQL_PARAM_INPUT_OUTPUT
            })
            .map(|(i, _)| i)
            .collect();

        let mut cursor = ProcCursor {
            stmt,
            buffers,
            out_indices,
            columns: Vec::new(),
            set_index: 0,
            all_done: false,
            out_params: None,
        };

        // Posicionar en el primer set con columnas (los vacios se saltan,
        // paridad con `call_proc` historico que solo pusheaba non-empty).
        let first = describe_columns(&cursor.stmt).unwrap_or_default();
        if !first.is_empty() {
            cursor.columns = first;
            return Ok(cursor);
        }
        loop {
            if !cursor.stmt.more_results()? {
                cursor.finish_out();
                return Ok(cursor);
            }
            let columns = describe_columns(&cursor.stmt).unwrap_or_default();
            if !columns.is_empty() {
                cursor.columns = columns;
                return Ok(cursor);
            }
        }
    }

    /// Ejecuta un `CALL schema.proc(?,...)` con bindeo por tipo de I/O
    /// (IN/INOUT/OUT) y trae:
    /// - todos los result sets (multiples via `SQLMoreResults`), y
    /// - los valores OUT/INOUT leidos de los buffers despues de ejecutar.
    ///
    /// Implementado sobre `call_proc_cursor` (un solo camino): drena set por
    /// set acumulando en `Vec`. Los llamadores que quieran pico de RAM bajo
    /// usan el cursor directo por lotes en vez de esta funcion.
    ///
    /// `metadata` es el resultado de `proc_columns` (mismo orden); `values`
    /// son los valores de entrada por posicion (`None` = NULL de entrada; los
    /// OUT pueden ir como `None` sin problema -- el driver escribe el
    /// resultado). Devuelve `(result_sets, out_params)` donde `out_params` es
    /// `(indice_en_metadata, texto_o_NULL)` para cada OUT/INOUT.
    pub fn call_proc(
        &self,
        schema: &str,
        proc_name: &str,
        metadata: &[ProcParam],
        values: &[Option<ParamValue>],
    ) -> Result<(CallResult, ProcOutParams), CoreError> {
        let mut cursor = self.call_proc_cursor(schema, proc_name, metadata, values)?;
        let mut result_sets = Vec::new();
        while !cursor.is_done() {
            let columns = cursor.current_columns().to_vec();
            if columns.is_empty() {
                if !cursor.advance()? {
                    break;
                }
                continue;
            }
            let mut rows = Vec::new();
            loop {
                let batch = cursor.fetch_batch(5000)?;
                if batch.is_empty() {
                    break;
                }
                rows.extend(batch);
            }
            result_sets.push((columns, rows));
            if !cursor.advance()? {
                break;
            }
        }
        let out_params = cursor.take_out_params().unwrap_or_default();
        Ok((result_sets, out_params))
    }
}

fn end_tran(hdbc: odbc_sys::HDbc, completion: odbc_sys::CompletionType) -> Result<(), CoreError> {
    let ret =
        unsafe { odbc_sys::SQLEndTran(odbc_sys::HandleType::Dbc, hdbc as *mut _, completion) };
    if ret == odbc_sys::SqlReturn::SUCCESS || ret == odbc_sys::SqlReturn::SUCCESS_WITH_INFO {
        return Ok(());
    }
    let diag = ffi::diag::primary_diagnostic(odbc_sys::HandleType::Dbc, hdbc as *mut _);
    Err(CoreError::from_diagnostic(diag))
}

// ---------------------------------------------------------------------------
// ConnManager -- deadpool::managed::Manager sobre RawConnection
// ---------------------------------------------------------------------------

pub struct ConnManager {
    dsn: SecretString,
    login_timeout_secs: u32,
}

impl managed::Manager for ConnManager {
    type Type = RawConnection;
    type Error = CoreError;

    async fn create(&self) -> Result<RawConnection, CoreError> {
        let dsn = self.dsn.expose_secret().to_string();
        let login_timeout = self.login_timeout_secs;
        tokio::task::spawn_blocking(move || {
            let env = ffi::environment()?;
            RawConnection::connect(env, &dsn, login_timeout)
        })
        .await
        .map_err(|e| CoreError::Connect(format!("panic creando conexion: {e}")))?
    }

    async fn recycle(
        &self,
        conn: &mut RawConnection,
        _metrics: &Metrics,
    ) -> RecycleResult<CoreError> {
        // Regla dura AGENTS.md ss4: conexion tainted (cancelada o que
        // sostuvo SESSION.*) o con SQL_ATTR_CONNECTION_DEAD -> se descarta,
        // nunca se recicla.
        if conn.tainted {
            return Err(RecycleError::message(
                "conexion marcada tainted (cancelada o con tabla de sesion abierta)",
            ));
        }
        let is_dead = conn.is_dead();
        if is_dead {
            return Err(RecycleError::message("SQL_ATTR_CONNECTION_DEAD"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Engine -- pool + DSN resuelto
// ---------------------------------------------------------------------------

/// Limites de statement/parametros descubiertos para este engine (ver
/// AGENTS.md ss9). Memoizado por engine (no global): cada `Engine` corresponde
/// a un DSN/conexion y los limites reales pueden variar entre sistemas.
///
/// Se mide en **unidades UTF-16 del statement generado**, no en filas: asi una
/// tabla angosta conserva lotes grandes aunque una ancha haya bajado el
/// limite (un tope global de filas se degradaba para todas las tablas por
/// igual). `budget_units()` combina la cota configurada, la mayor que se
/// observo funcionar y la menor que fallo.
#[derive(Debug, Default)]
pub struct StatementLimits {
    /// Cap configurado via `EngineOptions.max_statement_units`
    /// (`RUSTODBC_MAX_STATEMENT_UNITS`). Punto de partida; el cache nunca lo
    /// supera. `None` = sin cap configurado.
    pub configured_max_units: Option<usize>,
    /// Mayor largo de statement (unidades UTF-16) observado OK.
    pub max_statement_units: Option<usize>,
    /// Menor largo de statement observado que fallo por tamano.
    pub failed_statement_units: Option<usize>,
}

impl StatementLimits {
    /// Presupuesto util de unidades UTF-16: el minimo entre lo configurado,
    /// "lo mayor que funciono" y "lo menor que fallo - 1". `None` si todavia
    /// no hay ninguna observacion (el caller usa su tamano pedido).
    pub fn budget_units(&self) -> Option<usize> {
        let failed = self.failed_statement_units.map(|f| f.saturating_sub(1));
        [self.configured_max_units, self.max_statement_units, failed]
            .into_iter()
            .flatten()
            .min()
    }

    pub fn record_ok(&mut self, units: usize) {
        self.max_statement_units = Some(self.max_statement_units.map_or(units, |m| m.max(units)));
    }

    pub fn record_failed(&mut self, units: usize) {
        self.failed_statement_units =
            Some(self.failed_statement_units.map_or(units, |m| m.min(units)));
    }
}

pub struct Engine {
    pool: Pool<ConnManager>,
    /// Cache de limites de statement descubiertos por halve-and-retry.
    pub limits: std::sync::Arc<std::sync::Mutex<StatementLimits>>,
    /// Tamano del pool -- usado para acotar `workers` en la escritura masiva
    /// (mas workers que conexiones solo esperan en `acquire`).
    pub pool_size: usize,
}

impl Engine {
    pub fn connect(
        dsn: SecretString,
        pool_size: usize,
        login_timeout_secs: u32,
        max_statement_units: Option<usize>,
    ) -> Result<Self, CoreError> {
        // Fuerza la inicializacion del Environment singleton temprano, para
        // fallar rapido si el linkeo con odbc32/unixODBC esta roto, antes de
        // meterse en el pool.
        ffi::environment()?;

        let manager = ConnManager {
            dsn,
            login_timeout_secs,
        };
        let pool_size = pool_size.max(1);
        let pool = Pool::builder(manager)
            .max_size(pool_size)
            .runtime(deadpool::Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(30)))
            .build()
            .map_err(|e| CoreError::Configuration(format!("no se pudo armar el pool: {e}")))?;

        Ok(Engine {
            pool,
            limits: std::sync::Arc::new(std::sync::Mutex::new(StatementLimits {
                configured_max_units: max_statement_units,
                ..StatementLimits::default()
            })),
            pool_size,
        })
    }

    pub async fn acquire(&self) -> Result<Lease, CoreError> {
        let conn = self.pool.get().await.map_err(|e| match e {
            managed::PoolError::Timeout(_) => CoreError::PoolTimeout,
            other => CoreError::Connect(other.to_string()),
        })?;
        Ok(Lease { conn })
    }

    pub fn close(&self) {
        self.pool.close();
    }
}

/// Comparte el `Engine` entre las tareas async de tokio -- `Db2iEngine`
/// (capa PyO3) guarda un `Arc<Engine>` y lo clona en cada `spawn`.
pub type SharedEngine = Arc<Engine>;

/// Resultado de `Lease::call`: todos los result sets de un statement
/// multi-resultado (`(columnas, filas)` por cada uno).
pub type CallResult = Vec<(Vec<ColumnMeta>, Vec<Vec<ColumnValue>>)>;

/// Parametros OUT/INOUT de `Lease::call_proc`: `(indice_en_metadata,
/// texto_o_NULL)` para cada parametro de salida.
pub type ProcOutParams = Vec<(usize, Option<String>)>;

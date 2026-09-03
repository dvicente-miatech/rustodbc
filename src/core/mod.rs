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
struct ProcParamBuffer {
    _buf: Vec<u16>,
    _indicator: Box<odbc_sys::Len>,
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
        Some(ffi::wchar::from_utf16_lossy(&self._buf[..units]))
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
        SqlTypeFamily::Text | SqlTypeFamily::Clob => {
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

        // El C++ (cursor.cpp) toma COLUMN_SIZE del catalogo, capa a [256, 64KB],
        // y bindea todo como texto. Aca lo mismo pero UTF-16.
        let cap = p.column_size.clamp(1, 65536);
        let mut buf = vec![0u16; cap];
        let mut indicator = Box::new(odbc_sys::Len::default());

        // Escribir el valor de entrada si lo hay (IN/INOUT); si no, NULL.
        if is_in {
            if let Some(text) = param_value_to_text(values.get(i).and_then(|v| v.as_ref())) {
                let units = text.encode_utf16().collect::<Vec<_>>();
                let n = units.len().min(cap);
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
        });
    }

    Ok(buffers)
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
        stmt.exec_direct(sql)?;
        // Statements sin result set (INSERT/UPDATE/DELETE) o con: en ambos
        // casos SQLRowCount es valido segun la especificacion ODBC.
        stmt.row_count()
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

    /// Ejecuta un `CALL schema.proc(?,...)` con bindeo por tipo de I/O
    /// (IN/INOUT/OUT) y trae:
    /// - todos los result sets (multiples via `SQLMoreResults`), y
    /// - los valores OUT/INOUT leidos de los buffers despues de ejecutar.
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
        let placeholders = vec!["?"; metadata.len()].join(",");
        let sql = format!("{{CALL {schema}.{proc_name}({placeholders})}}");

        let stmt = RawStatement::alloc(self.hdbc())?;
        let buffers = bind_proc_params(&stmt, metadata, values)?;
        stmt.exec_direct(&sql)?;

        let mut result_sets = Vec::new();
        loop {
            let columns = describe_columns(&stmt).unwrap_or_default();
            if !columns.is_empty() {
                let mut rows = Vec::new();
                while stmt.fetch()? {
                    rows.push(fetch_row(&stmt, &columns)?);
                }
                result_sets.push((columns, rows));
            }
            if !stmt.more_results()? {
                break;
            }
        }

        // Leer OUT/INOUT de los buffers (que `bind_proc_params` mantuvo vivos).
        let mut out_params = Vec::new();
        for (i, p) in metadata.iter().enumerate() {
            if p.io_type == ffi::stmt::SQL_PARAM_OUTPUT
                || p.io_type == ffi::stmt::SQL_PARAM_INPUT_OUTPUT
            {
                out_params.push((i, buffers[i].read_out()));
            }
        }

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

/// Limites descubiertos de statement/parametros para este engine (ver
/// AGENTS.md ss9, halve-and-retry contra SQL0101/SQL54001). Memoizado por
/// engine (no global): cada `Engine` corresponde a un DSN/conexion y los
/// limites reales de DB2 for i pueden variar entre sistemas.
#[derive(Debug, Default)]
pub struct StatementLimits {
    /// Maximas filas por statement multi-row (por columna) que el driver
    /// acepta -- descubierto por halve-and-retry. `None` = todavia no se
    /// probo.
    pub max_rows_per_statement: Option<usize>,
}

pub struct Engine {
    pool: Pool<ConnManager>,
    /// Cache de limites de statement descubiertos por halve-and-retry.
    pub limits: std::sync::Arc<std::sync::Mutex<StatementLimits>>,
}

impl Engine {
    pub fn connect(
        dsn: SecretString,
        pool_size: usize,
        login_timeout_secs: u32,
    ) -> Result<Self, CoreError> {
        // Fuerza la inicializacion del Environment singleton temprano, para
        // fallar rapido si el linkeo con odbc32/unixODBC esta roto, antes de
        // meterse en el pool.
        ffi::environment()?;

        let manager = ConnManager {
            dsn,
            login_timeout_secs,
        };
        let pool = Pool::builder(manager)
            .max_size(pool_size.max(1))
            .runtime(deadpool::Runtime::Tokio1)
            .wait_timeout(Some(Duration::from_secs(30)))
            .build()
            .map_err(|e| CoreError::Configuration(format!("no se pudo armar el pool: {e}")))?;

        Ok(Engine {
            pool,
            limits: std::sync::Arc::new(std::sync::Mutex::new(StatementLimits::default())),
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

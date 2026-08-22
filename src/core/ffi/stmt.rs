//! `RawStatement`: wrapper seguro sobre un `HStmt`. Cada `RawStatement` es
//! propiedad exclusiva de quien la creo (una `ConnectionLease`, un
//! `BatchStream`); nunca se comparte entre tareas concurrentes salvo
//! `cancel()`, que ODBC permite explicitamente llamar desde otro hilo
//! mientras `SQLExecute`/`SQLFetch` esta en curso -- es la excepcion a la
//! regla de "un handle, un hilo" (ver AGENTS.md ss4 sobre cancelacion).

use std::ptr;

use odbc_sys::{
    CDataType, HDbc, HStmt, HandleType, Len, Nullability, ParamType, Pointer, SQLAllocHandle,
    SQLBindParameter, SQLCancel, SQLCloseCursor, SQLDescribeColW, SQLExecDirectW, SQLExecute,
    SQLFetch, SQLFreeHandle, SQLGetData, SQLMoreResults, SQLNumResultCols, SQLPrepareW,
    SQLRowCount, SqlDataType, SqlReturn, ULen,
};

use crate::errors::CoreError;

use super::diag::primary_diagnostic;
use super::wchar::{from_utf16_lossy, to_utf16, utf16_len};

/// Metadata cruda de una columna del result set (`SQLDescribeColW`).
/// `sql_type` queda como el codigo crudo (`SqlDataType.0`) -- la capa de
/// arriba (`rows.rs`) decide como interpretarlo (texto/numero/fecha/decimal)
/// sin que `core` conozca nada de Python.
#[derive(Debug, Clone)]
pub struct ColumnMeta {
    pub name: String,
    pub sql_type: i16,
    pub column_size: usize,
    pub decimal_digits: i16,
    pub nullable: bool,
}

/// Familia de tipos SQL, usada tanto por `stmt.rs` (para elegir el C type de
/// fetch) como por `rows.rs`/`params.rs` (para elegir la conversion a/desde
/// Python). Ver AGENTS.md ss4: DECIMAL/NUMERIC/DECFLOAT SIEMPRE como texto,
/// nunca float.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTypeFamily {
    Decimal,
    Integer,
    Float,
    Bit,
    Date,
    Time,
    Timestamp,
    Binary,
    Text,
}

pub fn classify_sql_type(sql_type: i16) -> SqlTypeFamily {
    match sql_type {
        t if t == SqlDataType::NUMERIC.0 || t == SqlDataType::DECIMAL.0 => SqlTypeFamily::Decimal,
        t if t == SqlDataType::INTEGER.0
            || t == SqlDataType::SMALLINT.0
            || t == SqlDataType::EXT_BIG_INT.0
            || t == SqlDataType::EXT_TINY_INT.0 =>
        {
            SqlTypeFamily::Integer
        }
        t if t == SqlDataType::FLOAT.0
            || t == SqlDataType::REAL.0
            || t == SqlDataType::DOUBLE.0 =>
        {
            SqlTypeFamily::Float
        }
        t if t == SqlDataType::EXT_BIT.0 => SqlTypeFamily::Bit,
        t if t == SqlDataType::DATE.0 => SqlTypeFamily::Date,
        t if t == SqlDataType::TIME.0 => SqlTypeFamily::Time,
        t if t == SqlDataType::TIMESTAMP.0 => SqlTypeFamily::Timestamp,
        t if t == SqlDataType::EXT_BINARY.0
            || t == SqlDataType::EXT_VAR_BINARY.0
            || t == SqlDataType::EXT_LONG_VAR_BINARY.0 =>
        {
            SqlTypeFamily::Binary
        }
        _ => SqlTypeFamily::Text,
    }
}

pub struct RawStatement {
    hstmt: HStmt,
}

// Ver nota de Send en `core::ffi::conn::RawConnection` -- mismo criterio:
// se mueve entre hilos (tareas de tokio), nunca se usa concurrentemente
// salvo `cancel()`, que ODBC define como seguro entre hilos.
unsafe impl Send for RawStatement {}

impl RawStatement {
    pub fn alloc(hdbc: HDbc) -> Result<Self, CoreError> {
        let mut hstmt: HStmt = ptr::null_mut();
        let ret = unsafe {
            SQLAllocHandle(
                HandleType::Stmt,
                hdbc as *mut _,
                &mut hstmt as *mut HStmt as *mut _,
            )
        };
        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            let diag = primary_diagnostic(HandleType::Dbc, hdbc as *mut _);
            return Err(CoreError::from_diagnostic(diag));
        }
        Ok(RawStatement { hstmt })
    }

    fn check(&self, ret: SqlReturn) -> Result<(), CoreError> {
        if ret == SqlReturn::SUCCESS || ret == SqlReturn::SUCCESS_WITH_INFO {
            return Ok(());
        }
        let diag = primary_diagnostic(HandleType::Stmt, self.hstmt as *mut _);
        Err(CoreError::from_diagnostic(diag))
    }

    pub fn exec_direct(&self, sql: &str) -> Result<(), CoreError> {
        let sql_u16 = to_utf16(sql);
        let ret = unsafe { SQLExecDirectW(self.hstmt, sql_u16.as_ptr(), utf16_len(sql) as i32) };
        self.check(ret)
    }

    pub fn prepare(&self, sql: &str) -> Result<(), CoreError> {
        let sql_u16 = to_utf16(sql);
        let ret = unsafe { SQLPrepareW(self.hstmt, sql_u16.as_ptr(), utf16_len(sql) as i32) };
        self.check(ret)
    }

    pub fn execute(&self) -> Result<(), CoreError> {
        let ret = unsafe { SQLExecute(self.hstmt) };
        self.check(ret)
    }

    pub fn num_result_cols(&self) -> Result<i16, CoreError> {
        let mut count: i16 = 0;
        let ret = unsafe { SQLNumResultCols(self.hstmt, &mut count) };
        self.check(ret)?;
        Ok(count)
    }

    /// `col` es 1-based, como toda la API de ODBC.
    pub fn describe_col(&self, col: u16) -> Result<ColumnMeta, CoreError> {
        let mut name_buf = [0u16; 256];
        let mut name_len: i16 = 0;
        let mut sql_type = SqlDataType::UNKNOWN_TYPE;
        let mut column_size: ULen = 0;
        let mut decimal_digits: i16 = 0;
        let mut nullable = Nullability::UNKNOWN;

        let ret = unsafe {
            SQLDescribeColW(
                self.hstmt,
                col,
                name_buf.as_mut_ptr(),
                name_buf.len() as i16,
                &mut name_len,
                &mut sql_type,
                &mut column_size,
                &mut decimal_digits,
                &mut nullable,
            )
        };
        self.check(ret)?;

        let len = (name_len.max(0) as usize).min(name_buf.len());
        Ok(ColumnMeta {
            name: from_utf16_lossy(&name_buf[..len]),
            sql_type: sql_type.0,
            column_size: column_size as usize,
            decimal_digits,
            nullable: nullable == Nullability::NULLABLE,
        })
    }

    /// `SQLFetch`. `Ok(true)` = hay fila, `Ok(false)` = `SQL_NO_DATA` (fin
    /// del result set).
    pub fn fetch(&self) -> Result<bool, CoreError> {
        let ret = unsafe { SQLFetch(self.hstmt) };
        if ret == SqlReturn::NO_DATA {
            return Ok(false);
        }
        self.check(ret)?;
        Ok(true)
    }

    /// Trae una columna completa como texto UTF-16 (`SQL_C_WCHAR`),
    /// reensamblando llamadas sucesivas de `SQLGetData` si el valor no
    /// entra en un solo buffer (LOBs / VARCHAR largos). `Ok(None)` = NULL.
    ///
    /// Se usa para TODO lo que no sea binario, incluyendo
    /// DECIMAL/NUMERIC/DECFLOAT (que llegan como el texto exacto del driver,
    /// nunca como float -- regla dura de AGENTS.md ss4) y fechas/horas (el
    /// driver las convierte a la representacion de caracteres estandar de
    /// ODBC: `YYYY-MM-DD`, `HH:MM:SS`, `YYYY-MM-DD HH:MM:SS[.ffffff]`).
    pub fn get_data_text(&self, col: u16) -> Result<Option<String>, CoreError> {
        const CHUNK: usize = 4096;
        const USABLE: usize = (CHUNK - 1) * std::mem::size_of::<u16>();
        let mut chunks: Vec<u16> = Vec::new();
        let mut got_any_call = false;

        loop {
            let mut buf = [0u16; CHUNK];
            let mut indicator: Len = 0;
            let ret = unsafe {
                SQLGetData(
                    self.hstmt,
                    col,
                    CDataType::WChar,
                    buf.as_mut_ptr() as Pointer,
                    (CHUNK * std::mem::size_of::<u16>()) as Len,
                    &mut indicator,
                )
            };

            if ret == SqlReturn::NO_DATA {
                // Todos los pedazos ya se entregaron en llamadas previas.
                break;
            }
            self.check(ret)?;
            got_any_call = true;

            if indicator == odbc_sys::NULL_DATA {
                return Ok(None);
            }

            // `indicator` viene en BYTES para SQL_C_WCHAR. Un valor negativo
            // distinto de NULL_DATA (SQL_NO_TOTAL) significa "trajo lo que
            // entro en el buffer, seguir pidiendo".
            let bytes_available = if indicator < 0 {
                USABLE
            } else {
                indicator as usize
            };
            let usable_units = bytes_available.min(USABLE) / std::mem::size_of::<u16>();
            chunks.extend_from_slice(&buf[..usable_units]);

            let fully_delivered = indicator >= 0 && (indicator as usize) < USABLE;
            if fully_delivered {
                break;
            }
        }

        if !got_any_call {
            return Ok(None);
        }
        Ok(Some(from_utf16_lossy(&chunks)))
    }

    /// Trae una columna binaria completa (`SQL_C_BINARY`), reensamblando
    /// llamadas sucesivas de `SQLGetData`. `Ok(None)` = NULL.
    pub fn get_data_binary(&self, col: u16) -> Result<Option<Vec<u8>>, CoreError> {
        const CHUNK: usize = 8192;
        let mut out: Vec<u8> = Vec::new();
        let mut saw_data = false;

        loop {
            let mut buf = [0u8; CHUNK];
            let mut indicator: Len = 0;
            let ret = unsafe {
                SQLGetData(
                    self.hstmt,
                    col,
                    CDataType::Binary,
                    buf.as_mut_ptr() as Pointer,
                    CHUNK as Len,
                    &mut indicator,
                )
            };

            if ret == SqlReturn::NO_DATA {
                break;
            }
            self.check(ret)?;

            if indicator == odbc_sys::NULL_DATA {
                if !saw_data {
                    return Ok(None);
                }
                break;
            }

            saw_data = true;
            let bytes_available = if indicator < 0 {
                CHUNK
            } else {
                indicator as usize
            };
            let usable = bytes_available.min(CHUNK);
            out.extend_from_slice(&buf[..usable]);

            if indicator >= 0 && (indicator as usize) < CHUNK {
                break;
            }
        }

        Ok(Some(out))
    }

    /// Bindea un parametro de entrada. `value` y `indicator` deben seguir
    /// vivos hasta despues de `execute()` -- `SQLBindParameter` solo
    /// registra punteros, el driver los lee recien al ejecutar. El llamador
    /// (`params.rs`) es responsable de mantener los buffers vivos en un
    /// `Vec` hasta que `execute()` retorne.
    ///
    /// `param` es 1-based.
    // Wrapper seguro sobre SQLBindParameter: los punteros se pasan al driver
    // (que los deref del otro lado), nunca se deref en Rust.
    #[allow(clippy::too_many_arguments, clippy::not_unsafe_ptr_arg_deref)]
    pub fn bind_input_parameter(
        &self,
        param: u16,
        c_type: CDataType,
        sql_type: SqlDataType,
        column_size: usize,
        decimal_digits: i16,
        value_ptr: Pointer,
        buffer_length: Len,
        indicator: *mut Len,
    ) -> Result<(), CoreError> {
        let ret = unsafe {
            SQLBindParameter(
                self.hstmt,
                param,
                ParamType::Input,
                c_type,
                sql_type,
                column_size as ULen,
                decimal_digits,
                value_ptr,
                buffer_length,
                indicator,
            )
        };
        self.check(ret)
    }

    pub fn row_count(&self) -> Result<i64, CoreError> {
        let mut count: Len = 0;
        let ret = unsafe { SQLRowCount(self.hstmt, &mut count) };
        self.check(ret)?;
        Ok(count as i64)
    }

    /// `SQLMoreResults` -- avanza al proximo result set de un statement
    /// multi-resultado (p.ej. un `CALL` a un procedimiento con varios
    /// `SELECT`). `Ok(true)` = hay otro result set, `Ok(false)` = no hay mas.
    pub fn more_results(&self) -> Result<bool, CoreError> {
        let ret = unsafe { SQLMoreResults(self.hstmt) };
        if ret == SqlReturn::NO_DATA {
            return Ok(false);
        }
        self.check(ret)?;
        Ok(true)
    }

    /// `SQLCancel` -- seguro de llamar desde otro hilo mientras este
    /// statement esta ejecutando (ver comentario del modulo). El pool
    /// descarta la conexion asociada despues de esto, nunca la recicla.
    pub fn cancel(&self) -> Result<(), CoreError> {
        let ret = unsafe { SQLCancel(self.hstmt) };
        self.check(ret)
    }

    pub fn close_cursor(&self) -> Result<(), CoreError> {
        let ret = unsafe { SQLCloseCursor(self.hstmt) };
        // SQL_ERROR aca casi siempre significa "no habia cursor abierto" en
        // drivers de AS400 -- no lo tratamos como fatal.
        if ret == SqlReturn::SUCCESS || ret == SqlReturn::SUCCESS_WITH_INFO {
            return Ok(());
        }
        Ok(())
    }
}

impl Drop for RawStatement {
    fn drop(&mut self) {
        // El retorno es `#[must_use]` (SqlReturn) -- descartar explicito.
        let _ = unsafe { SQLFreeHandle(HandleType::Stmt, self.hstmt as *mut _) };
    }
}

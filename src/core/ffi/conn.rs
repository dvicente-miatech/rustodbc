//! `RawConnection`: wrapper seguro sobre un `HDbc`. No es `Sync` -- un mismo
//! handle de conexion ODBC no se puede usar desde dos hilos a la vez (a
//! diferencia del handle de entorno). El pool (`pool.rs`) nunca comparte una
//! `RawConnection`; cada lease la posee en exclusiva.

use std::ptr;

use odbc_sys::{
    ConnectionAttribute, DriverConnectOption, HDbc, HandleType, SQLAllocHandle, SQLDisconnect,
    SQLDriverConnectW, SQLFreeHandle, SQLGetConnectAttr, SQLSetConnectAttr, SqlReturn,
};

use crate::errors::CoreError;

use super::diag::primary_diagnostic;
use super::env::Environment;
use super::wchar::{to_utf16, utf16_len};

/// `SQL_AUTOCOMMIT_OFF` / `SQL_AUTOCOMMIT_ON` -- valores crudos porque
/// `odbc-sys` 0.24 no los modela como enum (son valores del *value*, no del
/// *atributo*, de `SQLSetConnectAttr`).
const AUTOCOMMIT_OFF: odbc_sys::Pointer = 0 as odbc_sys::Pointer;
const AUTOCOMMIT_ON: odbc_sys::Pointer = 1 as odbc_sys::Pointer;

/// `SQL_CD_TRUE` -- valor devuelto por `SQL_ATTR_CONNECTION_DEAD` cuando la
/// conexion esta muerta (ver AGENTS.md ss4: "conexion cancelada se descarta,
/// nunca se recicla").
const CONNECTION_DEAD_TRUE: u32 = 1;

pub struct RawConnection {
    hdbc: HDbc,
    /// Marca de sesion: `true` si en algun momento se abrio una tabla
    /// temporal `SESSION.*` o se canceló un statement sobre esta conexion.
    /// El pool consulta esto en `recycle` para decidir si descartar en vez
    /// de reciclar (ver AGENTS.md ss4).
    pub tainted: bool,
}

// Ver justificacion de Send/Sync en `core::ffi::env::Environment`. Una
// `RawConnection` SI puede moverse entre hilos (se crea en un
// `spawn_blocking` y se devuelve al pool desde otro), pero nunca se usa
// concurrentemente desde dos hilos a la vez -- por eso `Send` sin `Sync`.
unsafe impl Send for RawConnection {}

impl RawConnection {
    /// Abre una conexion nueva usando `SQLDriverConnectW` con el DSN ya
    /// resuelto (ver `config::Credentials::build_dsn`). `login_timeout_secs
    /// == 0` significa "sin timeout", igual que el comportamiento original.
    pub fn connect(
        env: &Environment,
        dsn: &str,
        login_timeout_secs: u32,
    ) -> Result<Self, CoreError> {
        let mut hdbc: HDbc = ptr::null_mut();
        let ret = unsafe {
            SQLAllocHandle(
                HandleType::Dbc,
                env.handle() as *mut _,
                &mut hdbc as *mut HDbc as *mut _,
            )
        };
        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            return Err(CoreError::Connect(
                "no se pudo alocar el handle de conexion (SQLAllocHandle)".to_string(),
            ));
        }

        if login_timeout_secs > 0 {
            let _ = unsafe {
                SQLSetConnectAttr(
                    hdbc,
                    ConnectionAttribute::LoginTimeout,
                    login_timeout_secs as odbc_sys::Pointer,
                    0,
                )
            };
            // No fallamos la conexion si el driver ignora este atributo --
            // muchos drivers ODBC de mainframes/AS400 no lo soportan.
        }

        let dsn_u16 = to_utf16(dsn);
        let dsn_len = utf16_len(dsn);
        let mut out_buf = [0u16; 1024];
        let mut out_len: i16 = 0;

        let ret = unsafe {
            SQLDriverConnectW(
                hdbc,
                ptr::null_mut(),
                dsn_u16.as_ptr(),
                dsn_len,
                out_buf.as_mut_ptr(),
                out_buf.len() as i16,
                &mut out_len,
                DriverConnectOption::NoPrompt,
            )
        };

        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            let diag = primary_diagnostic(HandleType::Dbc, hdbc as *mut _);
            unsafe {
                SQLFreeHandle(HandleType::Dbc, hdbc as *mut _);
            }
            return Err(CoreError::from_diagnostic(diag));
        }

        Ok(RawConnection {
            hdbc,
            tainted: false,
        })
    }

    pub fn handle(&self) -> HDbc {
        self.hdbc
    }

    /// `SQL_ATTR_AUTOCOMMIT`. El pool mantiene todas las conexiones en
    /// autocommit por default; `Transaction` (en `engine.rs`) lo apaga sobre
    /// una `ConnectionLease` puntual y lo restaura al liberarla -- un solo
    /// pool, no dos (ver AGENTS.md ss "Concurrencia").
    pub fn set_autocommit(&self, on: bool) -> Result<(), CoreError> {
        let value = if on { AUTOCOMMIT_ON } else { AUTOCOMMIT_OFF };
        let ret =
            unsafe { SQLSetConnectAttr(self.hdbc, ConnectionAttribute::AutoCommit, value, 0) };
        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            let diag = primary_diagnostic(HandleType::Dbc, self.hdbc as *mut _);
            return Err(CoreError::from_diagnostic(diag));
        }
        Ok(())
    }

    /// Chequea `SQL_ATTR_CONNECTION_DEAD` antes de devolver una conexion al
    /// pool. Regla dura (AGENTS.md ss4): una conexion sospechosa se
    /// descarta, nunca se recicla.
    pub fn is_dead(&self) -> bool {
        let mut value: u32 = 0;
        let mut out_len: i32 = 0;
        let ret = unsafe {
            SQLGetConnectAttr(
                self.hdbc,
                ConnectionAttribute::ConnectionDead,
                &mut value as *mut u32 as odbc_sys::Pointer,
                std::mem::size_of::<u32>() as i32,
                &mut out_len,
            )
        };
        // Si el driver no soporta el atributo, asumimos viva -- no todos los
        // drivers de AS400 lo implementan; el `recycle` igual chequea
        // `tainted` por separado.
        ret == SqlReturn::SUCCESS && value == CONNECTION_DEAD_TRUE
    }
}

impl Drop for RawConnection {
    fn drop(&mut self) {
        unsafe {
            SQLDisconnect(self.hdbc);
            SQLFreeHandle(HandleType::Dbc, self.hdbc as *mut _);
        }
    }
}

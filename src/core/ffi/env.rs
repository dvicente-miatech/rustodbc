//! Handle de entorno ODBC: singleton por proceso, `SQL_OV_ODBC3`, y
//! enumeracion de drivers instalados (`SQLDriversW`) para autodetectar el
//! driver de `config::PREFERRED_DRIVERS` cuando no se pasa uno explicito.

use std::sync::OnceLock;

use odbc_sys::{
    AttrOdbcVersion, EnvironmentAttribute, FetchOrientation, HEnv, HandleType, SQLAllocHandle,
    SQLDriversW, SQLSetEnvAttr, SqlReturn,
};

use crate::errors::CoreError;

use super::diag::primary_diagnostic;

/// Wrapper `Send + Sync` sobre el `HEnv` crudo. Es valido compartirlo entre
/// hilos: el Driver Manager serializa el acceso al handle de entorno
/// internamente (ver especificacion ODBC 3.x, "Multithreading").
pub struct Environment {
    henv: HEnv,
}

// Seguro: el handle de entorno de ODBC esta disenado para uso concurrente
// por el Driver Manager (unixODBC/Windows DM). Los handles de conexion y
// statement, en cambio, NO son thread-safe y no se comparten (ver
// `core::conn`/`core::stmt`: cada uno vive detras de un `Lease` exclusivo).
unsafe impl Send for Environment {}
unsafe impl Sync for Environment {}

impl Environment {
    fn new() -> Result<Self, CoreError> {
        let mut henv: HEnv = std::ptr::null_mut();
        let ret = unsafe {
            SQLAllocHandle(
                HandleType::Env,
                std::ptr::null_mut(),
                &mut henv as *mut HEnv as *mut _,
            )
        };
        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            return Err(CoreError::Connect(
                "no se pudo alocar el handle de entorno ODBC (SQLAllocHandle)".to_string(),
            ));
        }

        let ret = unsafe {
            SQLSetEnvAttr(
                henv,
                EnvironmentAttribute::OdbcVersion,
                AttrOdbcVersion::Odbc3.into(),
                0,
            )
        };
        if ret != SqlReturn::SUCCESS && ret != SqlReturn::SUCCESS_WITH_INFO {
            let diag = primary_diagnostic(HandleType::Env, henv as *mut _);
            return Err(CoreError::from_diagnostic(diag));
        }

        Ok(Environment { henv })
    }

    pub fn handle(&self) -> HEnv {
        self.henv
    }

    /// Enumera los drivers ODBC instalados via `SQLDriversW(SQL_FETCH_FIRST /
    /// SQL_FETCH_NEXT)`. Usado para autodetectar el driver preferido cuando
    /// no se pasa `driver=` explicito ni esta seteado `DB_DRIVER` (ver
    /// `config::PREFERRED_DRIVERS`).
    pub fn list_drivers(&self) -> Vec<String> {
        use super::wchar::from_utf16_lossy;

        let mut drivers = Vec::new();
        let mut direction = FetchOrientation::First;

        loop {
            let mut desc = [0u16; 256];
            let mut desc_len: i16 = 0;
            let mut attrs = [0u16; 1024];
            let mut attrs_len: i16 = 0;

            let ret = unsafe {
                SQLDriversW(
                    self.henv,
                    direction,
                    desc.as_mut_ptr(),
                    desc.len() as i16,
                    &mut desc_len,
                    attrs.as_mut_ptr(),
                    attrs.len() as i16,
                    &mut attrs_len,
                )
            };

            match ret {
                SqlReturn::SUCCESS | SqlReturn::SUCCESS_WITH_INFO => {
                    let len = (desc_len.max(0) as usize).min(desc.len());
                    drivers.push(from_utf16_lossy(&desc[..len]));
                    direction = FetchOrientation::Next;
                }
                _ => break,
            }
        }

        drivers
    }
}

impl Drop for Environment {
    fn drop(&mut self) {
        // El retorno es `#[must_use]` (SqlReturn) -- descartar explicito.
        let _ = unsafe { odbc_sys::SQLFreeHandle(HandleType::Env, self.henv as *mut _) };
    }
}

static ENVIRONMENT: OnceLock<Result<Environment, String>> = OnceLock::new();

/// Devuelve el `Environment` singleton del proceso, inicializandolo en el
/// primer uso. Un solo `SQLAllocHandle(SQL_HANDLE_ENV)` por proceso -- ODBC
/// no espera (ni todos los drivers soportan bien) mas de uno.
pub fn environment() -> Result<&'static Environment, CoreError> {
    let result = ENVIRONMENT.get_or_init(|| Environment::new().map_err(|e| e.to_string()));
    result
        .as_ref()
        .map_err(|msg| CoreError::Connect(msg.clone()))
}

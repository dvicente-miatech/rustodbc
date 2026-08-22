//! Credenciales, opciones del engine, y resolucion de DSN por convencion de
//! entorno. Ver AGENTS.md ss5 para la tabla completa de variables.
//!
//! Regla dura: faltar una variable es un `ConfigurationError` que nombra TODAS
//! las que faltan -- nunca se construye `SYSTEM=None;UID=None;` en silencio
//! (ese era el comportamiento del original, `connection.py:30-35`).

use pyo3::prelude::*;
use secrecy::{ExposeSecret, SecretString};
use std::env;
use std::fmt;

use crate::errors::{to_py_err, CoreError};

/// Orden de preferencia para autodetectar el driver ODBC cuando no se pasa
/// `driver=` explicito ni esta seteado `DB_DRIVER`. El primer nombre que
/// `SQLDrivers` devuelva de esta lista gana (ver src/core/ffi, fase futura).
/// El default hardcodeado del original (`{iSeries Access ODBC Driver}`,
/// `connection.py:36`) es Windows-only y solo funcionaba en Linux porque
/// `DB_DRIVER` se fijaba a mano en cada despliegue.
pub const PREFERRED_DRIVERS: &[&str] = &[
    "IBM i Access ODBC Driver",
    "iSeries Access ODBC Driver",
    "Client Access ODBC Driver (32-bit)",
];

pub const DEFAULT_DRIVER_FALLBACK: &str = "{iSeries Access ODBC Driver}";

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

/// Credenciales de conexion a DB2 for i. El password vive en un
/// `SecretString`: no tiene getter Python y se redacta en `__repr__`.
///
/// `system`/`user`/`driver` son `Option` porque el camino
/// `from_connection_string` (DSN crudo, ver mas abajo) no garantiza poder
/// extraer esos keywords de forma confiable -- el DSN completo puede traer
/// keywords adicionales (`PORT=`, `CCSID=`, opciones propias del driver) que
/// esta capa no intenta parsear. Cuando vienen de `new`/`from_env`/`from_dsn`
/// siempre estan presentes.
#[pyclass(module = "rustodbc")]
pub struct Credentials {
    #[pyo3(get)]
    pub system: Option<String>,
    #[pyo3(get)]
    pub user: Option<String>,
    #[pyo3(get)]
    pub driver: Option<String>,
    pub(crate) password: SecretString,
    /// Si esta seteado, `build_dsn` lo devuelve verbatim, ignorando
    /// `system`/`user`/`driver`/`resolved_driver`. Ver `from_connection_string`.
    pub(crate) raw_dsn: Option<SecretString>,
}

impl fmt::Debug for Credentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Credentials")
            .field("system", &self.system)
            .field("user", &self.user)
            .field("driver", &self.driver)
            .field("password", &"<redacted>")
            .field("raw_dsn", &self.raw_dsn.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

#[pymethods]
impl Credentials {
    #[new]
    #[pyo3(signature = (*, system, user, password, driver=None))]
    fn new(system: String, user: String, password: String, driver: Option<String>) -> Self {
        Credentials {
            system: Some(system),
            user: Some(user),
            driver,
            password: SecretString::from(password),
            raw_dsn: None,
        }
    }

    /// Resuelve credenciales por la convencion `DB_SYSTEM_<CLIENTE>_<ENTORNO>`
    /// / `DB_USER_...` / `DB_PASSWORD_...`. `<ENTORNO>` sale de `environment`,
    /// si no de `APP_ENV`, si no `"dev"` -- ambas partes en mayusculas. Ver
    /// `connection.py:30-35` para el comportamiento exacto que esto preserva.
    #[staticmethod]
    #[pyo3(signature = (client_code, environment=None))]
    pub fn from_env(client_code: &str, environment: Option<&str>) -> PyResult<Self> {
        resolve_from_env(client_code, environment).map_err(to_py_err)
    }

    /// Escape hatch: DSN ODBC de 4 keywords conocidos
    /// (`DRIVER=;SYSTEM=;UID=;PWD=;`), para quien no quiera la convencion de
    /// entorno pero si quiere los campos individuales disponibles despues
    /// (`.system`, `.user`, `.driver`). Cualquier otro keyword en el DSN se
    /// descarta -- si necesitas preservarlos todos, usa
    /// `from_connection_string`.
    #[staticmethod]
    fn from_dsn(dsn: &str) -> PyResult<Self> {
        parse_dsn(dsn).map_err(to_py_err)
    }

    /// Escape hatch total: guarda `dsn` tal cual, sin parsear ni reconstruir.
    /// `build_dsn()` lo devuelve verbatim. Usar esto cuando el DSN trae
    /// keywords que esta libreria no modela explicitamente (`PORT=`,
    /// `CCSID=`, opciones propias del *IBM i Access ODBC Driver*, etc.) y no
    /// se puede perder ninguno en el camino. `.system`/`.user`/`.driver`
    /// quedan en `None` en este camino -- no se intenta adivinarlos.
    #[staticmethod]
    fn from_connection_string(dsn: &str) -> PyResult<Self> {
        if dsn.trim().is_empty() {
            return Err(to_py_err(CoreError::Configuration(
                "connection string vacia".to_string(),
            )));
        }
        Ok(Credentials {
            system: None,
            user: None,
            driver: None,
            password: SecretString::from(String::new()),
            raw_dsn: Some(SecretString::from(dsn.to_string())),
        })
    }

    fn __repr__(&self) -> String {
        if self.raw_dsn.is_some() {
            return "Credentials(raw_dsn=<redacted>)".to_string();
        }
        format!(
            "Credentials(system={:?}, user={:?}, driver={:?}, password=<redacted>)",
            self.system, self.user, self.driver
        )
    }
}

impl Credentials {
    /// Construye la cadena de conexion ODBC final. Si `Credentials` viene de
    /// `from_connection_string`, se devuelve ese DSN verbatim (ignorando
    /// `resolved_driver`). En cualquier otro camino, arma el DSN de 4
    /// keywords exacto que arma el original (`connection.py:53-59`): ese
    /// orden, punto y coma final. DB2 for i usa `SYSTEM=`, no `SERVER=`.
    pub fn build_dsn(&self, resolved_driver: &str) -> SecretString {
        if let Some(raw) = &self.raw_dsn {
            return raw.clone();
        }
        SecretString::from(format!(
            "DRIVER={};SYSTEM={};UID={};PWD={};",
            resolved_driver,
            self.system.as_deref().unwrap_or_default(),
            self.user.as_deref().unwrap_or_default(),
            self.password.expose_secret(),
        ))
    }

    pub fn password(&self) -> &SecretString {
        &self.password
    }
}

fn env_key(prefix: &str, client: &str, env_name: &str) -> String {
    format!("{prefix}_{client}_{env_name}")
}

fn resolve_from_env(
    client_code: &str,
    environment: Option<&str>,
) -> Result<Credentials, CoreError> {
    let client = client_code.to_uppercase();
    let env_name = environment
        .map(str::to_string)
        .or_else(|| env::var("APP_ENV").ok())
        .unwrap_or_else(|| "dev".to_string())
        .to_uppercase();

    let system_key = env_key("DB_SYSTEM", &client, &env_name);
    let user_key = env_key("DB_USER", &client, &env_name);
    let password_key = env_key("DB_PASSWORD", &client, &env_name);

    let system = env::var(&system_key).ok().filter(|s| !s.trim().is_empty());
    let user = env::var(&user_key).ok().filter(|s| !s.trim().is_empty());
    let password = env::var(&password_key)
        .ok()
        .filter(|s| !s.trim().is_empty());

    let mut missing = Vec::new();
    if system.is_none() {
        missing.push(system_key);
    }
    if user.is_none() {
        missing.push(user_key);
    }
    if password.is_none() {
        missing.push(password_key);
    }

    if !missing.is_empty() {
        return Err(CoreError::Configuration(format!(
            "faltan variables de entorno requeridas: {}",
            missing.join(", ")
        )));
    }

    let driver = env::var("DB_DRIVER").ok().filter(|s| !s.trim().is_empty());

    Ok(Credentials {
        system: Some(system.expect("checked above")),
        user: Some(user.expect("checked above")),
        driver,
        password: SecretString::from(password.expect("checked above")),
        raw_dsn: None,
    })
}

/// Parsea un DSN `DRIVER=...;SYSTEM=...;UID=...;PWD=...;` crudo. No valida
/// que sea exactamente ese orden de keywords -- ODBC no lo exige -- pero SI
/// requiere que SYSTEM/UID/PWD esten presentes.
fn parse_dsn(dsn: &str) -> Result<Credentials, CoreError> {
    let mut driver = None;
    let mut system = None;
    let mut user = None;
    let mut password = None;

    for part in dsn.split(';') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let Some((key, value)) = part.split_once('=') else {
            continue;
        };
        match key.trim().to_uppercase().as_str() {
            "DRIVER" => driver = Some(value.trim().to_string()),
            "SYSTEM" => system = Some(value.trim().to_string()),
            "UID" => user = Some(value.trim().to_string()),
            "PWD" => password = Some(value.trim().to_string()),
            _ => {}
        }
    }

    let (Some(system), Some(user), Some(password)) = (system, user, password) else {
        return Err(CoreError::Configuration(
            "DSN invalido: se esperaba SYSTEM=, UID= y PWD=".to_string(),
        ));
    };

    Ok(Credentials {
        system: Some(system),
        user: Some(user),
        driver,
        password: SecretString::from(password),
        raw_dsn: None,
    })
}

// ---------------------------------------------------------------------------
// EngineOptions
// ---------------------------------------------------------------------------

/// Tunables del engine. Los cinco primeros preservan exactamente los nombres
/// y defaults del original (`connection.py:40-44`); el default de
/// `max_workers` sigue el codigo (4), no el `.env.example` (16) -- ver
/// AGENTS.md ss4 "defectos a no portar".
#[pyclass(module = "rustodbc")]
#[derive(Debug, Clone)]
pub struct EngineOptions {
    #[pyo3(get, set)]
    pub pool_size: usize,
    #[pyo3(get, set)]
    pub login_timeout: u32,
    #[pyo3(get, set)]
    pub query_timeout: u32,
    #[pyo3(get, set)]
    pub batch_size: usize,
    #[pyo3(get, set)]
    pub max_workers: usize,
    #[pyo3(get, set)]
    pub min_rows_per_worker: usize,
    #[pyo3(get, set)]
    pub merge_chunk_size: usize,
    #[pyo3(get, set)]
    pub merge_max_workers: usize,
    #[pyo3(get, set)]
    pub stream_batch_size: usize,
    #[pyo3(get, set)]
    pub prefetch_batches: usize,
    #[pyo3(get, set)]
    pub decimal_mode: String, // "decimal" | "str" | "float"
    #[pyo3(get, set)]
    pub strip_char_padding: bool,
}

impl Default for EngineOptions {
    fn default() -> Self {
        EngineOptions {
            pool_size: 4,
            // 0 = sin timeout, igual que `timeout=0` del original
            // (connection.py:64,179) -- ver AGENTS.md sobre por que no es un
            // buen default a largo plazo, pero preserva el comportamiento hoy.
            login_timeout: 0,
            query_timeout: 0,
            batch_size: 1000,
            max_workers: 4,
            min_rows_per_worker: 500,
            merge_chunk_size: 7000,
            merge_max_workers: 3,
            stream_batch_size: 5000,
            prefetch_batches: 2,
            decimal_mode: "decimal".to_string(),
            strip_char_padding: true,
        }
    }
}

#[pymethods]
impl EngineOptions {
    #[new]
    #[pyo3(signature = (**kwargs))]
    fn new(kwargs: Option<&Bound<'_, pyo3::types::PyDict>>) -> PyResult<Self> {
        let mut opts = EngineOptions::default();
        if let Some(kwargs) = kwargs {
            for (key, value) in kwargs.iter() {
                let key: String = key.extract()?;
                apply_option(&mut opts, &key, value)?;
            }
        }
        Ok(opts)
    }

    /// Lee los cinco tunables compartidos + los especificos de rustodbc desde
    /// el entorno. Valores no numericos son `ConfigurationError`, nombrando la
    /// variable y el valor invalido (el original dejaba que `int(os.getenv())`
    /// tirara un `ValueError` opaco -- ver AGENTS.md "defectos a no portar").
    #[staticmethod]
    fn from_env() -> PyResult<Self> {
        let mut opts = EngineOptions::default();
        read_env_usize(&mut opts.batch_size, "BATCH_SIZE")?;
        read_env_usize(&mut opts.max_workers, "MAX_WORKERS")?;
        read_env_usize(&mut opts.min_rows_per_worker, "MIN_ROWS_PER_WORKER")?;
        read_env_usize(&mut opts.merge_chunk_size, "MERGE_CHUNK_SIZE")?;
        read_env_usize(&mut opts.merge_max_workers, "MERGE_MAX_WORKERS")?;
        read_env_usize(&mut opts.pool_size, "RUSTODBC_POOL_SIZE")?;
        Ok(opts)
    }
}

fn read_env_usize(field: &mut usize, key: &str) -> PyResult<()> {
    if let Ok(raw) = env::var(key) {
        let raw = raw.trim();
        if !raw.is_empty() {
            *field = raw.parse::<usize>().map_err(|_| {
                to_py_err(CoreError::Configuration(format!(
                    "{key}={raw:?} no es un entero valido"
                )))
            })?;
        }
    }
    Ok(())
}

fn apply_option(opts: &mut EngineOptions, key: &str, value: Bound<'_, PyAny>) -> PyResult<()> {
    macro_rules! set_usize {
        ($field:ident) => {{
            opts.$field = value.extract()?;
            return Ok(());
        }};
    }
    match key {
        "pool_size" => set_usize!(pool_size),
        "login_timeout" => {
            opts.login_timeout = value.extract()?;
            return Ok(());
        }
        "query_timeout" => {
            opts.query_timeout = value.extract()?;
            return Ok(());
        }
        "batch_size" => set_usize!(batch_size),
        "max_workers" => set_usize!(max_workers),
        "min_rows_per_worker" => set_usize!(min_rows_per_worker),
        "merge_chunk_size" => set_usize!(merge_chunk_size),
        "merge_max_workers" => set_usize!(merge_max_workers),
        "stream_batch_size" => set_usize!(stream_batch_size),
        "prefetch_batches" => set_usize!(prefetch_batches),
        "decimal_mode" => {
            opts.decimal_mode = value.extract()?;
            return Ok(());
        }
        "strip_char_padding" => {
            opts.strip_char_padding = value.extract()?;
            return Ok(());
        }
        other => {
            return Err(to_py_err(CoreError::Configuration(format!(
                "EngineOptions: opcion desconocida {other:?}"
            ))))
        }
    }
}

/// Carga un archivo `.env` -- SOLO si se llama explicitamente. `rustodbc`
/// nunca lo hace al importar (ver AGENTS.md ss5): una libreria que camina
/// directorios buscando `.env` es como se llega a "anda local, base
/// equivocada en prod".
#[pyfunction]
#[pyo3(signature = (path=None))]
pub fn load_dotenv(path: Option<&str>) -> bool {
    match path {
        Some(p) => dotenvy::from_path(p).is_ok(),
        None => dotenvy::dotenv().is_ok(),
    }
}

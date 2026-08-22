//! Jerarquia de excepciones expuesta a Python, y el enum de error interno de Rust.
//!
//! Regla dura (ver AGENTS.md ss8): ningun mensaje de error llega a Python sin
//! pasar por `scrub_password`. La cancelacion NUNCA entra en este arbol -- se
//! propaga como `asyncio.CancelledError`, no como un `RustOdbcError`.

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// ---------------------------------------------------------------------------
// Arbol de excepciones Python (rustodbc.<Nombre>)
// ---------------------------------------------------------------------------

create_exception!(rustodbc, RustOdbcError, PyException);

create_exception!(rustodbc, ConfigurationError, RustOdbcError);
create_exception!(rustodbc, ConnectError, RustOdbcError);
create_exception!(rustodbc, PoolTimeout, ConnectError);
create_exception!(rustodbc, InterfaceError, RustOdbcError);
create_exception!(rustodbc, QueryError, RustOdbcError);
create_exception!(rustodbc, SqlSyntaxError, QueryError); // SQLSTATE 42xxx
create_exception!(rustodbc, IntegrityError, QueryError); // SQLSTATE 23xxx
create_exception!(rustodbc, DataError, QueryError); // SQLSTATE 22xxx
create_exception!(rustodbc, OperationTimeout, QueryError); // HYT00 / HYT01
create_exception!(rustodbc, ParameterError, RustOdbcError);
create_exception!(rustodbc, BulkFailure, RustOdbcError);
create_exception!(rustodbc, MergeFailure, RustOdbcError);
create_exception!(rustodbc, CatalogError, RustOdbcError);
create_exception!(rustodbc, FeatureUnavailable, RustOdbcError);

/// Registra el arbol completo de excepciones en el modulo `#[pymodule]`.
pub fn register(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("RustOdbcError", py.get_type_bound::<RustOdbcError>())?;
    m.add(
        "ConfigurationError",
        py.get_type_bound::<ConfigurationError>(),
    )?;
    m.add("ConnectError", py.get_type_bound::<ConnectError>())?;
    m.add("PoolTimeout", py.get_type_bound::<PoolTimeout>())?;
    m.add("InterfaceError", py.get_type_bound::<InterfaceError>())?;
    m.add("QueryError", py.get_type_bound::<QueryError>())?;
    m.add("SqlSyntaxError", py.get_type_bound::<SqlSyntaxError>())?;
    m.add("IntegrityError", py.get_type_bound::<IntegrityError>())?;
    m.add("DataError", py.get_type_bound::<DataError>())?;
    m.add("OperationTimeout", py.get_type_bound::<OperationTimeout>())?;
    m.add("ParameterError", py.get_type_bound::<ParameterError>())?;
    m.add("BulkFailure", py.get_type_bound::<BulkFailure>())?;
    m.add("MergeFailure", py.get_type_bound::<MergeFailure>())?;
    m.add("CatalogError", py.get_type_bound::<CatalogError>())?;
    m.add(
        "FeatureUnavailable",
        py.get_type_bound::<FeatureUnavailable>(),
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Diagnostico ODBC (SQLGetDiagRec) -- llenado por src/core/ffi en fases futuras
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub sqlstate: String,
    pub native_code: i32,
    pub message: String,
}

/// Clasificacion de SQLCODEs de DB2 for i que SI importan en produccion (ver
/// AGENTS.md ss4 / plan "SQLCODEs que importan en produccion"). Nunca se
/// inspeccionaban programaticamente en el codigo original -- esto es la
/// especificacion de reintentos.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlCodeClass {
    /// SQL0913 (fila/objeto en uso) / SQL0904 (limite de recursos) -- reintentar
    /// con backoff exponencial.
    Transient,
    /// SQL0101 (statement demasiado largo) / SQL0104 (token invalido, p.ej.
    /// `IN ((?,?),...)`) / SQL0803 (clave duplicada) -- error tipado, no
    /// reintentar.
    Permanent,
    /// Cualquier otro SQLCODE -- se trata como permanente por default.
    Unknown,
}

/// `native_code` es el SQLCODE que trae el diagnostico ODBC (con signo, p.ej.
/// -913 para SQL0913). Ver `SqlCodeClass` para el porque de cada bucket.
pub fn classify_sqlcode(native_code: i32) -> SqlCodeClass {
    match native_code.abs() {
        913 | 904 => SqlCodeClass::Transient,
        101 | 104 | 803 => SqlCodeClass::Permanent,
        _ => SqlCodeClass::Unknown,
    }
}

/// SQLCODEs de DB2 for i que indican que el *statement* generado es demasiado
/// grande para este sistema (statement demasiado largo o demasiados
/// parametros) -- el halve-and-retry de chunk size (ver AGENTS.md ss9) los
/// usa para reducir el lote y reintentar en vez de abortar el batch entero.
pub fn is_reducible_size(native_code: i32) -> bool {
    matches!(native_code.abs(), 101 | 54001) // SQL0101 | SQL54001
}

/// Enum de error interno de Rust (capa `rustodbc-core`, sin PyO3). Se mapea a
/// una excepcion Python concreta en el borde de la capa PyO3 (`to_py_err`).
#[derive(thiserror::Error, Debug)]
pub enum CoreError {
    #[error("configuracion invalida: {0}")]
    Configuration(String),

    #[error("no se pudo conectar: {0}")]
    Connect(String),

    #[error("tiempo de espera agotado esperando una conexion del pool")]
    PoolTimeout,

    #[error("uso invalido de la interfaz: {0}")]
    Interface(String),

    #[error("error de consulta [{sqlstate}]: {message}")]
    Query {
        sqlstate: String,
        native_code: i32,
        message: String,
        diagnostics: Vec<Diagnostic>,
    },

    #[error("error de parametro: {0}")]
    Parameter(String),

    #[error("error de datos: {0}")]
    Data(String),

    #[error("error de catalogo: {0}")]
    Catalog(String),

    #[error("feature no disponible en este build: {0}")]
    FeatureUnavailable(String),
}

impl CoreError {
    /// Construye un `CoreError::Query` a partir de un diagnostico crudo,
    /// limpiando `PWD=` del mensaje antes de que llegue mas lejos.
    pub fn from_diagnostic(diag: Diagnostic) -> Self {
        CoreError::Query {
            sqlstate: diag.sqlstate.clone(),
            native_code: diag.native_code,
            message: scrub_password(&diag.message),
            diagnostics: vec![diag],
        }
    }

    pub fn sqlcode_class(&self) -> Option<SqlCodeClass> {
        match self {
            CoreError::Query { native_code, .. } => Some(classify_sqlcode(*native_code)),
            _ => None,
        }
    }

    /// `Some(native_code)` si es un `CoreError::Query` (SQLCODE de DB2 for i).
    pub fn native_code(&self) -> Option<i32> {
        match self {
            CoreError::Query { native_code, .. } => Some(*native_code),
            _ => None,
        }
    }
}

/// Convierte un `CoreError` en la excepcion Python concreta que le corresponde
/// en el arbol de arriba. Todo mensaje pasa por `scrub_password` antes de
/// llegar a Python -- regla dura, ver AGENTS.md ss8.
pub fn to_py_err(err: CoreError) -> PyErr {
    let msg = scrub_password(&err.to_string());
    match &err {
        CoreError::Configuration(_) => ConfigurationError::new_err(msg),
        CoreError::Connect(_) => ConnectError::new_err(msg),
        CoreError::PoolTimeout => PoolTimeout::new_err(msg),
        CoreError::Interface(_) => InterfaceError::new_err(msg),
        CoreError::Query { sqlstate, .. } => {
            let exc = match sqlstate.get(0..2) {
                Some("42") => SqlSyntaxError::new_err(msg),
                Some("23") => IntegrityError::new_err(msg),
                Some("22") => DataError::new_err(msg),
                _ if sqlstate == "HYT00" || sqlstate == "HYT01" => OperationTimeout::new_err(msg),
                _ => QueryError::new_err(msg),
            };
            exc
        }
        CoreError::Parameter(_) => ParameterError::new_err(msg),
        CoreError::Data(_) => DataError::new_err(msg),
        CoreError::Catalog(_) => CatalogError::new_err(msg),
        CoreError::FeatureUnavailable(_) => FeatureUnavailable::new_err(msg),
    }
}

/// Nunca dejar que `PWD=...` (ni ninguna variante con espacios/minusculas)
/// llegue a un mensaje de error visible desde Python. Ver AGENTS.md ss8.
pub fn scrub_password(text: &str) -> String {
    let re_prefixes = ["PWD=", "pwd=", "Pwd="];
    let mut out = text.to_string();
    for prefix in re_prefixes {
        while let Some(start) = out.find(prefix) {
            let value_start = start + prefix.len();
            let end = out[value_start..]
                .find(';')
                .map(|i| value_start + i)
                .unwrap_or(out.len());
            out.replace_range(value_start..end, "***");
        }
    }
    out
}

//! Conversion helpers UTF-8 <-> UTF-16 para las funciones `-W` de ODBC.
//!
//! `odbc_sys::WChar` esta fijo a `u16` en la version de la crate que usamos
//! (`odbc-sys 0.24`, ver `src/lib.rs` de la crate) -- no es el `wchar_t` de
//! 32 bits que preocupaba al `HACKING.md` del fork C++ para builds viejos de
//! unixODBC. Igual documentamos el riesgo (AGENTS.md ss9): si el *IBM i
//! Access ODBC Driver* en un unixODBC particular resulta compilado con
//! `SQL_WCHART_CONVERT` u otra convencion no-UTF16, esto se descubre recien
//! contra un sistema real -- no hay forma de verificarlo sin hardware.

use odbc_sys::WChar;

/// Convierte un `&str` de Rust a un buffer UTF-16 SIN terminador nulo -- las
/// funciones ODBC que usamos siempre reciben la longitud explicita
/// (`SQL_NTS` no se usa en este codigo para evitar depender de que el driver
/// respete null-termination en buffers UTF-16).
pub fn to_utf16(s: &str) -> Vec<WChar> {
    s.encode_utf16().collect()
}

/// Longitud en unidades UTF-16 (lo que ODBC espera en los parametros
/// `*_length` de las funciones `-W`), acotada a `i16` porque los parametros
/// de longitud de nombre de ODBC son `SmallInt`.
pub fn utf16_len(s: &str) -> i16 {
    s.encode_utf16().count() as i16
}

/// Decodifica un buffer UTF-16 crudo (posiblemente con basura despues del
/// largo real informado por el driver) a `String`, con reemplazo de
/// caracteres invalidos en vez de panicar -- un mensaje de diagnostico o un
/// valor de columna corrupto no debe tumbar el proceso.
pub fn from_utf16_lossy(buf: &[WChar]) -> String {
    String::from_utf16_lossy(buf)
}

//! Contencion del `unsafe` de `odbc-sys`. Nada fuera de `core::ffi` debe
//! llamar directo a una funcion `SQL*` -- todo pasa por los wrappers seguros
//! de estos submodulos.
//!
//! Disciplina de concurrencia (ver AGENTS.md ss4): ninguna funcion de este
//! modulo toca `Python::`/el GIL -- todo lo que entra aca corre dentro de un
//! `spawn_blocking` con el GIL ya liberado. El lint de CI
//! (`grep -rn "Python::" src/core/`) lo hace cumplir mecanicamente.

pub mod conn;
pub mod diag;
pub mod env;
pub mod stmt;
pub mod wchar;

pub use conn::RawConnection;
pub use env::{environment, Environment};
pub use stmt::{classify_sql_type, ColumnMeta, RawStatement, SqlTypeFamily};

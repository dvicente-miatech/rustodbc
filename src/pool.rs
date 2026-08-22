//! El pool de conexiones vive en `core::ConnManager` (implementa
//! `deadpool::managed::Manager` sobre `core::ffi::RawConnection`) -- no hay
//! logica PyO3 que agregar aca, asi que este archivo es solo un re-export
//! para que `crate::pool::*` siga siendo un nombre valido segun el mapa de
//! `src/` de AGENTS.md.
//!
//! Politica de recycle (ver AGENTS.md ss "Concurrencia", implementada en
//! `core::mod.rs::ConnManager::recycle`):
//! - Se descarta (nunca se recicla) una conexion `tainted` (cancelada, o que
//!   sostuvo una tabla temporal `SESSION.*`).
//! - Se descarta si `SQL_ATTR_CONNECTION_DEAD` indica que murio.
//! - `max_size = EngineOptions.pool_size` (default 4 -- deliberadamente NO
//!   cpu-aware, cada conexion es un job real en el AS/400).

pub use crate::core::{ConnManager, Engine, Lease, SharedEngine};

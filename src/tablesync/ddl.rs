//! DDL de la tabla temporal de sesion (`DECLARE GLOBAL TEMPORARY TABLE
//! SESSION.<temp>`) para el camino completo del motor MERGE original.
//!
//! **No implementado en esta primera pasada.** El `merge()` actual
//! (`merge.rs`) arma el `USING (VALUES ...)` directamente en el statement
//! `MERGE`, sin pasar por una tabla temporal de sesion -- mas simple y
//! evita la regla de "todo el trabajo con SESSION.* va pinneado a una
//! conexion" (AGENTS.md ss4) para esta version. El costo es que
//! `merge_chunk_size` limita cuantas filas entran en un solo `VALUES` (el
//! limite real de longitud de statement/parametros de DB2 for i no esta
//! descubierto todavia -- ver AGENTS.md ss9, "limites... halve-and-retry").
//! Migrar a tabla temporal + `ConnectionLease` pinneada es la version
//! completa, pendiente de validar contra un IBM i real.

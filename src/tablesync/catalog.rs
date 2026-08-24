//! Catalogo de claves primarias. Fuente primaria: las vistas de sistema
//! `QSYS2.SYSCST` / `QSYS2.SYSKEYCST` -- consultadas con SQL comun
//! (`SQLExecDirect`), no con las funciones de catalogo nativas de ODBC. Motivo:
//! `odbc-sys` 0.24 no expone `SQLPrimaryKeys` (la funcion FFI no esta en esta
//! version de la crate, ver AGENTS.md ss9), y consultar el catalogo por SQL es
//! ademas mas portable entre versiones de DB2 for i.
//!
//! Fallback: `SYSCST`/`SYSKEYCST` solo ven constraints del SQL schema (DDL o
//! `ADDPFCST TYPE(*PRIKEY)` explicito). Los **PFs nativos creados por DDS**
//! (clave `K`, aunque tengan `UNIQUE`) no figuran ahi -- su clave se expone via
//! `SYSIBM.SQLSPECIALCOLUMNS` (el espejo ODBC de `SQLSpecialColumns` / "best row
//! id"). Cuando la fuente primaria devuelve vacio, se consulta el fallback (con
//! dedupe por `SCOPE`, ya que la vista puede repetir columnas).

use std::collections::HashSet;

use crate::core::{ColumnValue, Lease, ParamValue};
use crate::errors::CoreError;

const PK_QUERY: &str = "\
SELECT kc.COLUMN_NAME \
FROM QSYS2.SYSCST c \
JOIN QSYS2.SYSKEYCST kc \
  ON c.CONSTRAINT_NAME = kc.CONSTRAINT_NAME AND c.TABLE_SCHEMA = kc.TABLE_SCHEMA \
WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.CONSTRAINT_TYPE = 'PRIMARY KEY' \
ORDER BY kc.ORDINAL_POSITION";

/// Fallback para PFs nativos (DDS). Misma forma que la funcion de catalogo ODBC
/// `SQLSpecialColumns`, validada contra un sistema real.
const PK_QUERY_SPECIALCOLUMNS: &str = "\
SELECT COLUMN_NAME \
FROM SYSIBM.SQLSPECIALCOLUMNS \
WHERE TABLE_SCHEM = ? AND TABLE_NAME = ?";

/// Devuelve las columnas de la PK de `schema.table`, en orden ordinal.
/// `Vec` vacio = la tabla no tiene PK/indice unico en el catalogo -- regla
/// dura de AGENTS.md ss4: sin PK, `merge()` degrada a INSERT con warning,
/// NUNCA crashea y NUNCA hace un MERGE silencioso sin clave.
pub fn primary_key_columns(
    lease: &Lease,
    schema: &str,
    table: &str,
) -> Result<Vec<String>, CoreError> {
    let params = vec![
        ParamValue::Text(schema.to_string()),
        ParamValue::Text(table.to_string()),
    ];

    // 1) PK declarada via SQL schema (DDL / ADDPFCST): ordenada por
    //    ORDINAL_POSITION.
    let (_columns, rows) = lease.query(PK_QUERY, &params)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(ColumnValue::Text(name)) = row.first() {
            out.push(name.clone());
        }
    }
    if !out.is_empty() {
        return Ok(out);
    }

    // 2) Fallback para PFs nativos (DDS): la clave no es una constraint del
    //    SQL schema, pero si aparece como "best row id" en SQLSPECIALCOLUMNS.
    //    La vista puede devolver la misma columna con distinto SCOPE (rowid de
    //    sesion vs. transaccion) -- se deduplica conservando el primer orden.
    let (_columns, rows) = lease.query(PK_QUERY_SPECIALCOLUMNS, &params)?;
    let mut seen = HashSet::with_capacity(rows.len());
    for row in rows {
        if let Some(ColumnValue::Text(name)) = row.first() {
            if seen.insert(name.clone()) {
                out.push(name);
            }
        }
    }
    Ok(out)
}

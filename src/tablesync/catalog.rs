//! Catalogo de claves primarias via las vistas de sistema `QSYS2.SYSCST` /
//! `QSYS2.SYSKEYCST` -- consultadas con SQL comun (`SQLExecDirect`), no con
//! las funciones de catalogo nativas de ODBC. Motivo: `odbc-sys` 0.24 no
//! expone `SQLPrimaryKeys` (la funcion FFI no esta en esta version de la
//! crate, ver AGENTS.md ss9), y consultar el catalogo por SQL es ademas mas
//! portable entre versiones de DB2 for i.

use crate::core::{ColumnValue, Lease, ParamValue};
use crate::errors::CoreError;

const PK_QUERY: &str = "\
SELECT kc.COLUMN_NAME \
FROM QSYS2.SYSCST c \
JOIN QSYS2.SYSKEYCST kc \
  ON c.CONSTRAINT_NAME = kc.CONSTRAINT_NAME AND c.TABLE_SCHEMA = kc.TABLE_SCHEMA \
WHERE c.TABLE_SCHEMA = ? AND c.TABLE_NAME = ? AND c.CONSTRAINT_TYPE = 'PRIMARY KEY' \
ORDER BY kc.ORDINAL_POSITION";

/// Devuelve las columnas de la PK de `schema.table`, en orden ordinal.
/// `Vec` vacio = la tabla no tiene PK/indice unico en el catalogo -- regla
/// dura de AGENTS.md ss4: sin PK, `merge()` degrada a INSERT con warning,
/// NUNCA crashea y NUNCA hace un MERGE silencioso sin clave.
pub fn primary_key_columns(lease: &Lease, schema: &str, table: &str) -> Result<Vec<String>, CoreError> {
    let params = vec![
        ParamValue::Text(schema.to_string()),
        ParamValue::Text(table.to_string()),
    ];
    let (_columns, rows) = lease.query(PK_QUERY, &params)?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(ColumnValue::Text(name)) = row.first() {
            out.push(name.clone());
        }
    }
    Ok(out)
}

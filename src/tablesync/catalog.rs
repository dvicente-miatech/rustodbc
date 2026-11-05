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

use std::collections::{HashMap, HashSet};

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

/// Tipos declarados de la tabla destino. Db2 for i **no** acepta marcadores de
/// parametro sin tipo dentro de un `VALUES` que alimenta un MERGE (SQL0584: "En
/// VALUES, no se permite NULL, UNKNOWN o un marcador de parametro"), asi que el
/// MERGE necesita el tipo real de cada columna para armarlo como
/// `CAST(? AS <tipo>)`.
const COLUMN_TYPES_QUERY: &str = "\
SELECT COLUMN_NAME, DATA_TYPE, LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE \
FROM QSYS2.SYSCOLUMNS \
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?";

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
    //    HashSet<String> (owned): las filas se consumen en el loop y no pueden
    //    prestar &str a un set que sobrevive a la iteracion.
    let (_columns, rows) = lease.query(PK_QUERY_SPECIALCOLUMNS, &params)?;
    let mut seen: HashSet<String> = HashSet::with_capacity(rows.len());
    for row in rows {
        if let Some(ColumnValue::Text(name)) = row.first() {
            if seen.insert(name.clone()) {
                out.push(name.clone());
            }
        }
    }
    Ok(out)
}

/// Tipo SQL de cada columna de `schema.table` (nombre en mayusculas -> texto de
/// tipo), para armar `CAST(? AS <tipo>)` en el `VALUES` del MERGE.
///
/// Un tipo no mapeado queda **fuera** del mapa: su marcador va sin `CAST` y Db2
/// devuelve SQL0584, que es el mismo error de antes. Se prefiere eso a inventar
/// un tipo (ROWID/XML/DISTINCT no son escribibles con un CAST generico).
pub fn column_cast_types(
    lease: &Lease,
    schema: &str,
    table: &str,
) -> Result<HashMap<String, String>, CoreError> {
    let params = vec![
        ParamValue::Text(schema.to_string()),
        ParamValue::Text(table.to_string()),
    ];

    let (_columns, rows) = lease.query(COLUMN_TYPES_QUERY, &params)?;
    let mut out = HashMap::with_capacity(rows.len());

    for row in rows {
        let Some(ColumnValue::Text(name)) = row.first() else {
            continue;
        };
        let Some(ColumnValue::Text(data_type)) = row.get(1) else {
            continue;
        };
        if let Some(sql_type) = cast_type(
            data_type,
            as_i64(row.get(2)),
            as_i64(row.get(3)),
            as_i64(row.get(4)),
        ) {
            out.insert(name.to_uppercase(), sql_type);
        }
    }
    Ok(out)
}

/// Filas del catalogo: los numeros llegan como texto (`ColumnValue::Text`).
fn as_i64(value: Option<&ColumnValue>) -> Option<i64> {
    match value {
        Some(ColumnValue::Text(text)) => text.trim().parse().ok(),
        _ => None,
    }
}

/// `DATA_TYPE` de `QSYS2.SYSCOLUMNS` -> tipo valido en un `CAST` (con longitud,
/// precision y escala, que son las que Db2 exige en ese contexto).
fn cast_type(
    data_type: &str,
    length: Option<i64>,
    precision: Option<i64>,
    scale: Option<i64>,
) -> Option<String> {
    let data_type = data_type.trim().to_uppercase();
    let length = length.unwrap_or(0).max(0);
    let precision = precision.unwrap_or(0).clamp(1, 31);
    let scale = scale.unwrap_or(0).max(0).min(precision);

    Some(match data_type.as_str() {
        "CHAR" | "CHARACTER" => format!("CHAR({})", length.clamp(1, 32766)),
        "VARCHAR" => format!("VARCHAR({})", length.clamp(1, 32740)),
        "GRAPHIC" => format!("GRAPHIC({})", length.clamp(1, 16383)),
        "VARGRAPHIC" => format!("VARGRAPHIC({})", length.clamp(1, 16370)),
        // DECIMAL = packed, NUMERIC = zoned. Se preserva el tipo declarado.
        "DECIMAL" | "ZONED" => format!("DECIMAL({precision},{scale})"),
        "NUMERIC" | "PACKED" => format!("NUMERIC({precision},{scale})"),
        "SMALLINT" => "SMALLINT".to_string(),
        "INTEGER" => "INTEGER".to_string(),
        "BIGINT" => "BIGINT".to_string(),
        "REAL" => "REAL".to_string(),
        "DOUBLE" | "FLOAT" => "DOUBLE".to_string(),
        "DECFLOAT" => format!("DECFLOAT({})", if precision > 16 { 34 } else { 16 }),
        "DATE" => "DATE".to_string(),
        "TIME" => "TIME".to_string(),
        // Db2 for i acepta TIMESTAMP(n); el texto del driver llega con
        // microsegundos, que es justo la precision 6.
        "TIMESTAMP" => "TIMESTAMP(6)".to_string(),
        "BINARY" => format!("BINARY({})", length.clamp(1, 32766)),
        "VARBINARY" => format!("VARBINARY({})", length.clamp(1, 32740)),
        "CLOB" => format!("CLOB({})", length.clamp(1, 16_773_120)),
        // `LENGTH` de un DBCLOB viene en bytes: 2 por caracter DBCS.
        "DBCLOB" => format!("DBCLOB({})", (length / 2).clamp(1, 8_386_560)),
        "BLOB" => format!("BLOB({})", length.clamp(1, 16_773_120)),
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::cast_type;

    #[test]
    fn tipos_de_columna_se_traducen_a_un_cast_valido() {
        // Los tipos reales de un PF de produccion (PRAXISMP.MPF102 en TEST).
        assert_eq!(
            cast_type("CHAR", Some(10), None, None).as_deref(),
            Some("CHAR(10)")
        );
        assert_eq!(
            cast_type("NUMERIC", Some(13), Some(13), Some(2)).as_deref(),
            Some("NUMERIC(13,2)")
        );
        assert_eq!(
            cast_type("DECIMAL", Some(7), Some(7), Some(3)).as_deref(),
            Some("DECIMAL(7,3)")
        );
        assert_eq!(
            cast_type("VARCHAR", Some(40), None, None).as_deref(),
            Some("VARCHAR(40)")
        );
        assert_eq!(cast_type("DATE", None, None, None).as_deref(), Some("DATE"));
        assert_eq!(
            cast_type("TIMESTAMP", None, None, None).as_deref(),
            Some("TIMESTAMP(6)")
        );
        assert_eq!(
            cast_type("DECFLOAT", None, Some(34), None).as_deref(),
            Some("DECFLOAT(34)")
        );
        // Escala mayor que precision no debe generar `DECIMAL(2,5)`.
        assert_eq!(
            cast_type("DECIMAL", Some(2), Some(2), Some(5)).as_deref(),
            Some("DECIMAL(2,2)")
        );
        // Sin tipo conocido no se inventa un CAST.
        assert_eq!(cast_type("ROWID", None, None, None), None);
    }
}

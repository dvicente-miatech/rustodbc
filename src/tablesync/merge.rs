//! Arma y ejecuta el `MERGE INTO ... USING (VALUES ...) AS s(...)`.
//!
//! Reglas duras aplicadas aca (ver AGENTS.md ss4 y ss8 "defectos a no
//! portar"):
//! - Tabla toda-PK (sin columnas no-PK) generaria un `WHEN MATCHED THEN
//!   UPDATE SET` vacio -- SQL invalido. Se detecta y se omite la clausula
//!   `WHEN MATCHED` entera (el MERGE queda insert-only).
//! - Sin PK: esta funcion no se llama -- el caller (`tablesync/mod.rs`)
//!   degrada a `INSERT` simple con warning antes de llegar aca.
//! - Sin exito parcial silencioso: cada chunk que falla propaga el error tal
//!   cual, sin intentar seguir con los chunks restantes.
//! - Los limites de statement (SQL0101/SQL54001 de DB2, HY090 del Driver
//!   Manager) se manejan con halve-and-retry compartido con `executebatch`
//!   (ver `crate::bulk::execute_chunked`).
//! - Cada marca del `VALUES` va como `CAST(? AS <tipo>)`: Db2 for i rechaza un
//!   `VALUES` con marcadores sin tipo cuando alimenta un MERGE (SQL0584). Los
//!   tipos los da el catalogo destino (`catalog::column_cast_types`).

use crate::bulk::execute_chunked;
use crate::core::{Lease, ParamValue, StatementLimits};
use crate::errors::CoreError;

fn quote_ident(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// Un grupo `(?, ...)` del `VALUES`; cada marca lleva su `CAST` cuando se
/// conoce el tipo destino (obligatorio en el `VALUES` de un MERGE).
fn row_values_placeholder(column_count: usize, casts: &[String]) -> String {
    let marks: Vec<String> = (0..column_count)
        .map(|i| match casts.get(i).map(String::as_str) {
            Some(sql_type) if !sql_type.is_empty() => format!("CAST(? AS {sql_type})"),
            _ => "?".to_string(),
        })
        .collect();
    format!("({})", marks.join(","))
}

/// Arma el `MERGE` para un chunk de `chunk_rows` filas. `columns` es el
/// orden canonico (de las keys del primer dict de Python, ver
/// `tablesync/mod.rs`); `pk_columns` debe ser un subconjunto de `columns`.
///
/// `casts` va alineado con `columns`: el tipo SQL de cada columna destino, o
/// vacio si se desconoce. Un `VALUES` que alimenta un MERGE no admite
/// marcadores sin tipo (SQL0584), por eso cada marca va como
/// `CAST(? AS <tipo>)`.
fn build_merge_sql(
    schema: &str,
    table: &str,
    pk_columns: &[String],
    columns: &[String],
    casts: &[String],
    chunk_rows: usize,
) -> String {
    let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let cols_quoted: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let non_pk: Vec<&String> = columns.iter().filter(|c| !pk_columns.contains(c)).collect();

    let row_placeholder = row_values_placeholder(columns.len(), casts);
    let values_clause = vec![row_placeholder; chunk_rows.max(1)].join(",");

    let on_clause = pk_columns
        .iter()
        .map(|pk| format!("t.{} = s.{}", quote_ident(pk), quote_ident(pk)))
        .collect::<Vec<_>>()
        .join(" AND ");

    let insert_cols = cols_quoted.join(",");
    let insert_values = columns
        .iter()
        .map(|c| format!("s.{}", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(",");

    let matched_clause = if non_pk.is_empty() {
        // Tabla toda-PK: sin columnas para actualizar -- MERGE insert-only.
        String::new()
    } else {
        let set_clause = non_pk
            .iter()
            .map(|c| format!("t.{} = s.{}", quote_ident(c), quote_ident(c)))
            .collect::<Vec<_>>()
            .join(",");
        format!("WHEN MATCHED THEN UPDATE SET {set_clause} ")
    };

    format!(
        "MERGE INTO {qualified_table} AS t \
         USING (VALUES {values_clause}) AS s({cols}) \
         ON ({on_clause}) \
         {matched_clause}\
         WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_values})",
        cols = cols_quoted.join(","),
    )
}

/// Builder del `MERGE INTO ... USING (VALUES ...) AS s(...)` para N filas.
/// `Send + Sync` para poder correrlo en workers.
pub(crate) fn merge_sql_builder(
    schema: String,
    table: String,
    pk_columns: Vec<String>,
    columns: Vec<String>,
    casts: Vec<String>,
) -> impl Fn(usize) -> String + Send + Sync {
    move |n: usize| build_merge_sql(&schema, &table, &pk_columns, &columns, &casts, n)
}

/// Builder del `INSERT ... VALUES (CAST(? AS t),...)` (sin PK -- ver regla dura
/// de arriba) para N filas. `Send + Sync` para poder correrlo en workers.
pub(crate) fn insert_sql_builder(
    schema: String,
    table: String,
    columns: Vec<String>,
    casts: Vec<String>,
) -> impl Fn(usize) -> String + Send + Sync {
    let qualified_table = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
    let cols_quoted: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let row_placeholder = row_values_placeholder(columns.len(), &casts);
    move |n: usize| {
        let values_clause = vec![row_placeholder.clone(); n.max(1)].join(",");
        format!(
            "INSERT INTO {qualified_table} ({}) VALUES {values_clause}",
            cols_quoted.join(",")
        )
    }
}

/// Ejecuta el MERGE en chunks de `chunk_size` filas, con halve-and-retry de
/// tamano (memoizado en `limits`). Devuelve `(rows_affected_total, batches)`.
#[allow(clippy::too_many_arguments)]
pub fn merge_rows(
    lease: &Lease,
    limits: &std::sync::Mutex<StatementLimits>,
    schema: &str,
    table: &str,
    pk_columns: &[String],
    columns: &[String],
    casts: &[String],
    rows: &[Vec<ParamValue>],
    chunk_size: usize,
) -> Result<(i64, usize), CoreError> {
    let build = merge_sql_builder(
        schema.to_string(),
        table.to_string(),
        pk_columns.to_vec(),
        columns.to_vec(),
        casts.to_vec(),
    );
    execute_chunked(lease, limits, &build, rows, chunk_size)
}

/// INSERT simple (sin PK -- ver regla dura de arriba), en los mismos chunks,
/// con halve-and-retry compartido.
#[allow(clippy::too_many_arguments)]
pub fn insert_only_rows(
    lease: &Lease,
    limits: &std::sync::Mutex<StatementLimits>,
    schema: &str,
    table: &str,
    columns: &[String],
    casts: &[String],
    rows: &[Vec<ParamValue>],
    chunk_size: usize,
) -> Result<(i64, usize), CoreError> {
    let build = insert_sql_builder(
        schema.to_string(),
        table.to_string(),
        columns.to_vec(),
        casts.to_vec(),
    );
    execute_chunked(lease, limits, &build, rows, chunk_size)
}

#[cfg(test)]
mod tests {
    use super::{build_merge_sql, insert_sql_builder, row_values_placeholder};

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn el_values_del_merge_lleva_el_tipo_de_cada_columna() {
        let columns = cols(&["BANDOC", "DATECI", "NETO"]);
        let casts = cols(&["CHAR(10)", "CHAR(8)", "NUMERIC(13,2)"]);

        // Tipos reales de PRAXISMP.MPF102 en el sistema de TEST.
        let sql = build_merge_sql(
            "PRAXISMP",
            "MPF102",
            &cols(&["BANDOC", "DATECI"]),
            &columns,
            &casts,
            2,
        );

        assert!(
            sql.contains(
                "USING (VALUES (CAST(? AS CHAR(10)),CAST(? AS CHAR(8)),\
                 CAST(? AS NUMERIC(13,2))),\
                 (CAST(? AS CHAR(10)),CAST(? AS CHAR(8)),CAST(? AS NUMERIC(13,2)))) \
                 AS s(\"BANDOC\",\"DATECI\",\"NETO\")"
            ),
            "{sql}"
        );
        // Ninguna marca sin tipo: Db2 for i responde SQL0584 si queda una.
        assert_eq!(sql.matches('?').count(), 6, "{sql}");
        assert_eq!(sql.matches("CAST(?").count(), 6, "{sql}");
    }

    #[test]
    fn columna_sin_tipo_queda_sin_cast_y_tabla_toda_pk_es_insert_only() {
        let sql = build_merge_sql(
            "S",
            "T",
            &cols(&["ID", "VALOR"]),
            &cols(&["ID", "VALOR"]),
            &cols(&["", "VARCHAR(10)"]),
            1,
        );
        assert!(
            sql.contains("USING (VALUES (?,CAST(? AS VARCHAR(10))))"),
            "{sql}"
        );
        // Sin columnas fuera de la PK no hay nada que actualizar.
        assert!(!sql.contains("WHEN MATCHED"), "{sql}");
    }

    #[test]
    fn insert_sin_pk_tambien_lleva_cast() {
        let build = insert_sql_builder(
            "S".to_string(),
            "T".to_string(),
            cols(&["ID"]),
            cols(&["BIGINT"]),
        );
        assert_eq!(
            build(2),
            "INSERT INTO \"S\".\"T\" (\"ID\") VALUES (CAST(? AS BIGINT)),(CAST(? AS BIGINT))"
        );
    }

    #[test]
    fn placeholder_sin_tipos_es_el_de_siempre() {
        assert_eq!(row_values_placeholder(3, &[]), "(?,?,?)");
    }
}

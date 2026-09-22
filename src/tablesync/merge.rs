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

use crate::bulk::execute_chunked;
use crate::core::{Lease, ParamValue, StatementLimits};
use crate::errors::CoreError;

fn quote_ident(id: &str) -> String {
    format!("\"{}\"", id.replace('"', "\"\""))
}

/// Arma el `MERGE` para un chunk de `chunk_rows` filas. `columns` es el
/// orden canonico (de las keys del primer dict de Python, ver
/// `tablesync/mod.rs`); `pk_columns` debe ser un subconjunto de `columns`.
fn build_merge_sql(
    schema: &str,
    table: &str,
    pk_columns: &[String],
    columns: &[String],
    chunk_rows: usize,
) -> String {
    let qualified_table = format!("{}.{}", quote_ident(schema), quote_ident(table));
    let cols_quoted: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let non_pk: Vec<&String> = columns.iter().filter(|c| !pk_columns.contains(c)).collect();

    let row_placeholder = format!("({})", vec!["?"; columns.len()].join(","));
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
) -> impl Fn(usize) -> String + Send + Sync {
    move |n: usize| build_merge_sql(&schema, &table, &pk_columns, &columns, n)
}

/// Builder del `INSERT ... VALUES (?,...),(?,...)` (sin PK -- ver regla dura
/// de arriba) para N filas. `Send + Sync` para poder correrlo en workers.
pub(crate) fn insert_sql_builder(
    schema: String,
    table: String,
    columns: Vec<String>,
) -> impl Fn(usize) -> String + Send + Sync {
    let qualified_table = format!("{}.{}", quote_ident(&schema), quote_ident(&table));
    let cols_quoted: Vec<String> = columns.iter().map(|c| quote_ident(c)).collect();
    let row_placeholder = format!("({})", vec!["?"; columns.len()].join(","));
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
    rows: &[Vec<ParamValue>],
    chunk_size: usize,
) -> Result<(i64, usize), CoreError> {
    let build = merge_sql_builder(
        schema.to_string(),
        table.to_string(),
        pk_columns.to_vec(),
        columns.to_vec(),
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
    rows: &[Vec<ParamValue>],
    chunk_size: usize,
) -> Result<(i64, usize), CoreError> {
    let build = insert_sql_builder(schema.to_string(), table.to_string(), columns.to_vec());
    execute_chunked(lease, limits, &build, rows, chunk_size)
}

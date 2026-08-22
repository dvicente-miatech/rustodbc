//! Export opcional via la Arrow C Data Interface (`__arrow_c_stream__`).
//! Feature `arrow`, apagada por default -- cero consumidores hoy (`fetch_df`
//! no tenia llamadores en el codigo original). Fase 12.
//!
//! - `Decimal128Array` con precision/escala del catalogo; `Decimal256` para
//!   39-76 digitos; string con warning para DECFLOAT o mas de 76 digitos --
//!   degradar en silencio es peor que tipar como texto.
//! - Ni `pyarrow` ni `polars` son dependencia de este crate: quien reciba el
//!   stream hace `pyarrow.table(stream)` / `polars.from_arrow(stream)`
//!   zero-copy por su cuenta.

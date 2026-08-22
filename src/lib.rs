//! `rustodbc` -- motor ODBC async para IBM DB2 for i (iSeries), expuesto como
//! extension de Python via PyO3. Ver AGENTS.md para el diseno completo.
//!
//! Estado (ver AGENTS.md "Plan de implementacion" para el detalle fase por
//! fase): `config`/`errors` (Fase 0), `core::ffi` + `core` (Fase 1-4),
//! `params`/`rows` (Fase 3/5), `engine`/`stream` (Fase 2-4) estan
//! implementados. `bulk`/`proc`/`tablesync` tienen una primera
//! implementacion funcional. `blocking` sigue como stub deliberado (Fase
//! 11, se completa recien cuando la API async este validada contra un IBM i
//! real -- ver AGENTS.md ss9).

pub mod config;
pub mod errors;

#[cfg(feature = "arrow")]
pub mod arrow;
pub mod blocking;
pub mod bulk;
pub mod core;
pub mod engine;
pub mod params;
pub mod pool;
pub mod proc;
pub mod rows;
pub mod stream;
#[cfg(feature = "tablesync")]
pub mod tablesync;

use pyo3::prelude::*;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[pymodule]
#[pyo3(name = "_rustodbc")]
fn rustodbc_native(py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    pyo3_log::init();

    m.add("__version__", VERSION)?;

    errors::register(py, m)?;

    m.add_class::<config::Credentials>()?;
    m.add_class::<config::EngineOptions>()?;
    m.add_function(wrap_pyfunction!(config::load_dotenv, m)?)?;

    m.add_class::<engine::Db2iEngine>()?;
    m.add_class::<stream::BatchStream>()?;

    m.add_class::<bulk::BulkReport>()?;
    m.add_class::<bulk::TaskFailure>()?;
    m.add_class::<bulk::ParallelReport>()?;
    m.add_function(wrap_pyfunction!(bulk::plan_concurrency, m)?)?;

    m.add_class::<proc::ProcResult>()?;

    #[cfg(feature = "tablesync")]
    {
        m.add_class::<tablesync::TableSync>()?;
        m.add_class::<tablesync::MergeReport>()?;
    }

    Ok(())
}

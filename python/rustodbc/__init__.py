"""rustodbc -- motor ODBC async para IBM DB2 for i (iSeries).

Ver AGENTS.md para el diseno completo. Este paquete es solo el `__init__`
que re-exporta la superficie curada del modulo compilado `._rustodbc`.
"""

from ._rustodbc import (
    __version__ as __version__,
)
from ._rustodbc import (
    BatchStream,
    BulkFailure,
    BulkReport,
    CatalogError,
    ConfigurationError,
    ConnectError,
    Credentials,
    DataError,
    Db2iEngine,
    EngineOptions,
    FeatureUnavailable,
    InterfaceError,
    IntegrityError,
    MergeFailure,
    OperationTimeout,
    ParallelReport,
    ParameterError,
    PoolTimeout,
    ProcResult,
    ProcValidationError,
    QueryError,
    RustOdbcError,
    SqlSyntaxError,
    TaskFailure,
    load_dotenv,
    plan_concurrency,
)

try:
    from ._rustodbc import MergeReport, TableSync
except ImportError:
    # Build sin la feature Cargo "tablesync" (default on, pero separable --
    # ver AGENTS.md ss2).
    MergeReport = None  # type: ignore[assignment]
    TableSync = None  # type: ignore[assignment]


def _db2i_engine_stream(self, sql=None, params=None, batch_size=None):
    """Itera fila por fila (azucar sobre stream_batches, que entrega lotes).

    Uso:
        async for row in engine.stream("SELECT ..."):
            ...
    """

    async def _gen():
        async for batch in self.stream_batches(sql, params, batch_size):
            for row in batch:
                yield row

    return _gen()


# Pisa el metodo nativo `stream` (alias de stream_batches) por el generador
# de filas. Las clases pyo3 SI aceptan asignacion de atributos a nivel clase.
Db2iEngine.stream = _db2i_engine_stream  # type: ignore[attr-defined]

__all__ = [
    "__version__",
    "Credentials",
    "EngineOptions",
    "load_dotenv",
    "Db2iEngine",
    "BatchStream",
    "BulkReport",
    "TaskFailure",
    "ParallelReport",
    "plan_concurrency",
    "ProcResult",
    "MergeReport",
    "TableSync",
    "RustOdbcError",
    "ConfigurationError",
    "ConnectError",
    "PoolTimeout",
    "InterfaceError",
    "QueryError",
    "SqlSyntaxError",
    "IntegrityError",
    "DataError",
    "OperationTimeout",
    "ParameterError",
    "ProcValidationError",
    "BulkFailure",
    "MergeFailure",
    "CatalogError",
    "FeatureUnavailable",
]

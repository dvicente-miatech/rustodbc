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
    "BulkFailure",
    "MergeFailure",
    "CatalogError",
    "FeatureUnavailable",
]

"""rustodbc.blocking -- fachada sincrona (BlockingEngine).

Se importa por submodulo explicito a proposito: nadie deberia depender de
`blocking` sin pedirlo.

Uso:

    from rustodbc.blocking import BlockingEngine

    engine = BlockingEngine.from_env("ACME", "PROD")
    rows = engine.fetch_all("SELECT * FROM SCHEMA.TABLA WHERE id = ?", [123])
    engine.close()
"""

from ._rustodbc import BlockingBatchStream as BlockingBatchStream
from ._rustodbc import BlockingEngine as BlockingEngine

__all__ = ["BlockingEngine", "BlockingBatchStream"]

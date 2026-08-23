# rustodbc

Motor ODBC async en Rust para IBM DB2 for i (iSeries / AS400), expuesto como
extensión de Python (PyO3 + maturin).

Ver [AGENTS.md](AGENTS.md) para el diseño completo y el estado real de cada
módulo.

> **Estado del proyecto:** API **async** (`Db2iEngine`) y **síncrona**
> (`BlockingEngine` vía `rustodbc.blocking`) implementadas, CI verde
> (`cargo fmt` + `clippy -D warnings` + build + smoke import en Windows y
> Linux). Streaming con prefetch en ambas.

## Instalación

Una vez publicado en PyPI:

```bash
pip install rustodbc-mi
```

**El paquete de PyPI se llama `rustodbc-mi`** (así está registrado el trusted
publisher en pypi.org), pero el `import` en Python es `rustodbc`.

**El wheel solo no alcanza en runtime.** DB2 for i se habla a través del
driver *IBM i Access ODBC Driver*, que no viene con el wheel (ver "Requisito
de runtime" más abajo).

## Build local

```powershell
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo build --release
maturin develop --release
python -c "import rustodbc; print(rustodbc.__version__)"
```

Requiere: Rust estable (target `x86_64-pc-windows-msvc` en Windows), maturin,
y el toolchain de C++ de Visual Studio (`link.exe`, `odbc32.lib`) en Windows o
`unixodbc-dev`/`unixODBC-devel` en Linux.

---

## Conexión: dos caminos

`rustodbc` no asume nunca una única forma de conectar. Hay dos caminos
independientes, y podés mezclarlos según el caso.

### 1. Convención de entorno (`Credentials.from_env`)

Pensada para multi-cliente/multi-entorno: las credenciales viven en variables
de entorno con el patrón `<VAR>_<CLIENTE>_<ENTORNO>`, nunca en código ni en
config versionada.

```python
import rustodbc

# Lee DB_SYSTEM_ACME_PROD, DB_USER_ACME_PROD, DB_PASSWORD_ACME_PROD
creds = rustodbc.Credentials.from_env("ACME", "PROD")
```

Variables que lee (ver también [.env.example](.env.example)):

| Variable | Requerida | Descripción |
|---|---|---|
| `DB_SYSTEM_<CLIENTE>_<ENTORNO>` | Sí | Hostname/IP del AS/400 |
| `DB_USER_<CLIENTE>_<ENTORNO>` | Sí | Usuario |
| `DB_PASSWORD_<CLIENTE>_<ENTORNO>` | Sí | Password |
| `APP_ENV` | No | `<ENTORNO>` default si no se pasa `environment=` (default `"dev"`) |
| `DB_DRIVER` | No | Nombre exacto del driver ODBC; si falta, se autodetecta con `SQLDrivers` (ver `config.rs::PREFERRED_DRIVERS`) |

`<CLIENTE>` y `<ENTORNO>` se normalizan siempre a mayúsculas. Si falta
**cualquiera** de las tres variables requeridas, se levanta
`ConfigurationError` nombrando **todas** las que faltan (nunca se conecta con
`SYSTEM=None;UID=None;` en silencio):

```python
try:
    creds = rustodbc.Credentials.from_env("ACME", "PROD")
except rustodbc.ConfigurationError as e:
    print(e)  # "faltan variables de entorno requeridas: DB_SYSTEM_ACME_PROD, DB_PASSWORD_ACME_PROD"
```

`rustodbc` **nunca carga un `.env` implícitamente**. Si tu app usa archivos
`.env`, cargalo vos explícitamente antes:

```python
rustodbc.load_dotenv()            # busca .env en el directorio actual/padres
rustodbc.load_dotenv("/ruta/a/.env")  # o una ruta explícita
```

### 2. Connection string directo

Para quien no quiere (o no puede) usar la convención de entorno: tests
locales, un DSN de un solo uso, un secret manager propio, etc. Hay **tres**
variantes, de más estructurada a más cruda:

**a) Constructor directo** — cuando ya tenés los campos por separado:

```python
creds = rustodbc.Credentials(
    system="10.0.0.5",
    user="MIUSER",
    password="secreto",
    driver="IBM i Access ODBC Driver",  # opcional; se autodetecta si se omite
)
```

**b) `Credentials.from_dsn`** — parsea un DSN de los 4 keywords conocidos
(`DRIVER=`, `SYSTEM=`, `UID=`, `PWD=`) y te deja `.system`/`.user`/`.driver`
disponibles después:

```python
creds = rustodbc.Credentials.from_dsn(
    "DRIVER={IBM i Access ODBC Driver};SYSTEM=10.0.0.5;UID=MIUSER;PWD=secreto;"
)
creds.system  # "10.0.0.5"
```

Cualquier keyword que no sea uno de esos 4 se **descarta** — si tu connection
string trae `PORT=`, `CCSID=`, u otras opciones propias del driver, usá la
variante (c).

**c) `Credentials.from_connection_string`** — guarda el string **tal cual**,
sin parsear ni reconstruir. Es el escape hatch total: nada se pierde, pero
`.system`/`.user`/`.driver` quedan en `None` porque no se intenta adivinarlos:

```python
creds = rustodbc.Credentials.from_connection_string(
    "DRIVER={IBM i Access ODBC Driver};SYSTEM=10.0.0.5;PORT=8471;"
    "UID=MIUSER;PWD=secreto;CCSID=37;"
)
```

En los tres casos, `repr(creds)` nunca expone el password (ni el DSN crudo
completo en el caso (c), justamente porque puede contener `PWD=`).

---

## `EngineOptions`

Tunables del engine — pool, batching, paralelismo, formato de datos. Todos
tienen default y son ajustables por keyword o por variable de entorno.

```python
opts = rustodbc.EngineOptions(pool_size=8, batch_size=2000)
# o
opts = rustodbc.EngineOptions.from_env()
```

| Campo | Default | Env var | Descripción |
|---|---|---|---|
| `pool_size` | 4 | `RUSTODBC_POOL_SIZE` | Conexiones simultáneas en el pool |
| `login_timeout` | 0 (sin timeout) | — | Timeout de `SQLConnect`, en segundos |
| `query_timeout` | 0 (sin timeout) | — | Timeout de ejecución de statement, en segundos |
| `batch_size` | 1000 | `BATCH_SIZE` | Filas por lote en `executebatch`/inserts masivos |
| `max_workers` | 4 | `MAX_WORKERS` | Jobs paralelos máximos (I/O-bound, **no** cpu-aware — ver AGENTS.md ss4) |
| `min_rows_per_worker` | 500 | `MIN_ROWS_PER_WORKER` | Umbral para decidir cuántos workers usar |
| `merge_chunk_size` | 7000 | `MERGE_CHUNK_SIZE` | Filas por chunk en el motor MERGE (`TableSync`) |
| `merge_max_workers` | 3 | `MERGE_MAX_WORKERS` | Jobs paralelos máximos para MERGE |
| `stream_batch_size` | 5000 | — | Filas por lote al iterar con `stream`/`stream_batches` |
| `prefetch_batches` | 2 | — | Lotes prefetcheados por delante durante streaming |
| `decimal_mode` | `"decimal"` | — | `"decimal"` (siempre `Decimal` exacto) / `"str"` / `"float"` |
| `strip_char_padding` | `True` | — | Recorta el relleno de espacios de columnas `CHAR`/`GRAPHIC` |

`EngineOptions.from_env()` solo lee las variables que tienen equivalente en
la tabla; un valor no numérico levanta `ConfigurationError` nombrando la
variable y el valor inválido (no un `ValueError` opaco).

> Nota sobre `decimal_mode`: `rustodbc` bindea `DECIMAL`/`NUMERIC`/`DECFLOAT`
> como texto crudo del driver y lo pasa a `decimal.Decimal(...)` — nunca
> float. Es una regla dura del proyecto (ver AGENTS.md ss4), no solo un
> default conveniente.

---

## Árbol de excepciones

Todas heredan de `rustodbc.RustOdbcError` (que a su vez es `Exception`).
Ningún mensaje de error llega a Python sin pasar por un scrub que redacta
`PWD=...` — nunca vas a ver una contraseña en un traceback.

```
RustOdbcError
├── ConfigurationError       # credenciales/opciones faltantes o inválidas
├── ConnectError             # fallo de SQLConnect
│   └── PoolTimeout          # se agotó el tiempo esperando una conexión libre del pool
├── InterfaceError           # uso inválido de la API (p.ej. conexión ya cerrada)
├── QueryError               # error de ejecución de SQL
│   ├── SqlSyntaxError       # SQLSTATE 42xxx
│   ├── IntegrityError       # SQLSTATE 23xxx (p.ej. violación de PK/FK)
│   ├── DataError            # SQLSTATE 22xxx (p.ej. overflow, conversión inválida)
│   └── OperationTimeout     # SQLSTATE HYT00 / HYT01
├── ParameterError           # parámetro Python no representable en un tipo ODBC
├── BulkFailure              # uno o más statements de un batch fallaron
├── MergeFailure             # el motor MERGE (TableSync) falló
├── CatalogError             # no se pudo leer el catálogo (PK, columnas, tipos)
└── FeatureUnavailable       # feature no compilada en este wheel (p.ej. arrow)
```

Notar: la **cancelación nunca entra en este árbol** — se propaga como
`asyncio.CancelledError` nativo, no como una excepción de `rustodbc`.

```python
try:
    ...
except rustodbc.IntegrityError:
    ...  # violación de PK/FK -- no reintentar
except rustodbc.QueryError as e:
    print(e.sqlstate, e.native_code, e.message)
```

---

## Uso async

La API es async (basada en asyncio + un runtime tokio interno). Todo el ciclo
de vida del engine debe correr dentro de **un solo event loop** — usá un único
`asyncio.run(main())` o un único `loop.run_until_complete(...)`, nunca
`asyncio.run()` por llamada (ver "Errores comunes" más abajo).

```python
import asyncio
import rustodbc

async def main():
    async with rustodbc.Db2iEngine.from_env("ACME", "PROD") as engine:
        # ejecutar sin traer datos -> rowcount
        n = await engine.execute("UPDATE SCHEMA.TABLA SET x = ? WHERE id = ?", [1, 123])

        # traer todo -> list[dict]
        rows = await engine.fetch_all("SELECT * FROM SCHEMA.TABLA WHERE id = ?", [123])
        # rows[0] == {"ID": 123, "NOMBRE": "..."}

        # una fila -> dict | None
        row = await engine.fetch_one("SELECT * FROM SCHEMA.TABLA WHERE id = ?", [123])

        # un solo valor / una columna
        total = await engine.fetch_value("SELECT COUNT(*) FROM SCHEMA.TABLA")
        ids = await engine.fetch_column("SELECT id FROM SCHEMA.TABLA")

        # streaming por lotes (no carga todo en memoria) -> list[Row] por batch.
        # Prefetch: mientras consumís un lote, la tarea ya pidió el siguiente
        # (EngineOptions.prefetch_batches, default 2) -- RAM acotada a unos
        # pocos lotes, no a toda la tabla.
        async for batch in engine.stream_batches("SELECT * FROM SCHEMA.HUGE_TABLE", batch_size=5000):
            for r in batch:
                ...

        # azúcar: `stream()` itera fila por fila (mismo batching interno,
        # misma RAM acotada) sobre stream_batches.
        async for row in engine.stream("SELECT * FROM SCHEMA.HUGE_TABLE"):
            ...

asyncio.run(main())
```

Conexión explícita (en vez de `async with`):

```python
engine = await rustodbc.Db2iEngine.connect(creds)   # o .from_env("ACME", "PROD")
try:
    rows = await engine.fetch_all(sql)
finally:
    engine.close()   # idempotente
```

### Parámetros

Posicionales (`list`/`tuple` o `None`). Tipos: `str`, `int`, `float`, `bool`,
`decimal.Decimal`, `bytes`/`bytearray`, `date`/`time`/`datetime`, `None`.
`DECIMAL`/`NUMERIC`/`DECFLOAT` llegan siempre como `Decimal` exacto (nunca
float), con `decimal_mode="decimal"` por defecto.

```python
from decimal import Decimal
from datetime import date

await engine.execute(
    "INSERT INTO T (importe, fecha, ok) VALUES (?, ?, ?)",
    [Decimal("123.45"), date(2026, 8, 22), True],
)
```

### Escritura masiva (`executebatch`)

Reescribe `INSERT ... VALUES (?,...)` a un `VALUES` multi-fila (el driver IBM i
no soporta `SQL_ATTR_PARAMSET_SIZE`). **Un solo lease** para todo el batch
(no uno por lote) y **halve-and-retry de chunk size**: si un lote excede el
límite de statement/parámetros de DB2 for i (SQL0101/SQL54001), se reduce a
la mitad y se reintenta; el tamaño que funcionó queda cacheado por engine y
se usa solo a partir de entonces. Además, los errores **transitorios**
(SQL0913/SQL0904: fila/objeto en uso, límite de recursos) se reintentan con
backoff creciente (3 reintentos) — como la BD no es transaccional, un chunk
fallido no deja estado parcial.

```python
report = await engine.executebatch(
    "INSERT INTO SCHEMA.TABLA (a, b) VALUES (?,?)",
    [[1, "x"], [2, "y"], [3, "z"]],
)
print(report.rows_affected, report.batches)
```

### Escritura masiva en paralelo (`batch_execute` / `parallel_execute`)

Reparten la carga en varias conexiones del pool (workers), cada una con su
propio lease. Sin éxito parcial silencioso: se juntan **todos** los errores
antes de devolver el reporte. `fail_fast=True` (opt-in) cancela el resto al
primer error.

```python
# batch_execute: un solo INSERT, filas partidas en workers
report = await engine.batch_execute(
    "INSERT INTO SCHEMA.TABLA (a, b) VALUES (?,?)",
    [[1, "x"], [2, "y"], [3, "z"], [4, "w"]],
    max_workers=4,      # default: EngineOptions.max_workers
    fail_fast=False,    # default
)
print(report.rows_affected, report.failures)

# parallel_execute: varias (sql, rows) independientes en paralelo
report = await engine.parallel_execute([
    ("INSERT INTO SCHEMA.A (id) VALUES (?)", [[1], [2]]),
    ("INSERT INTO SCHEMA.B (id) VALUES (?)", [[3], [4]]),
], max_workers=2)
print(report.rows_affected, report.failures)
```

### Procedimientos (`call_proc`)

Recibe un **dict** `{nombre_parametro: valor}`. Los parámetros **OUT no
necesitan venir** en el dict — se bindean como NULL de entrada y el
procedimiento igual se ejecuta; el resultado sale en `out_params`:

```python
result = await engine.call_proc("SCHEMA", "MI_PROC", {"IN_PARAM": 5})
# result.result_sets -> list[list[Row]]
# result.out_params  -> {"OUT_PARAM": <valor>, ...}  # OUT/INOUT
```

### MERGE/upsert (`TableSync`)

Por composición, nunca herencia:

```python
class TransferRepository:
    def __init__(self, ori, dest):
        self.sync = dest.table_sync(source=ori)

sync = dest_engine.table_sync(source=ori_engine)
report = await sync.merge(
    "SCHEMA", "TABLA",
    [{"ID": 1, "VALOR": "a"}, {"ID": 2, "VALOR": "b"}],
    # primary_key=["ID"],   # opcional; si se omite, lo saca del catálogo
)
print(report.used_merge, report.rows_affected, report.warning)
```

Sin PK → hace INSERT simple con `warning` (nunca crashea ni hace MERGE
silencioso — regla dura de AGENTS.md ss4).

#### Copiar tabla desde otro engine (`transfer`)

Lee `source_schema.source_table` desde el engine `source` **en streaming**
(RAM acotada por lote) y la mergea/inserta en `dest_schema.dest_table` del
engine `dest`. Origen y destino pueden ser esquemas/tablas distintos.
`select_sql` opcional permite un SELECT con filtro (debe devolver las mismas
columnas que la tabla destino):

```python
sync = dest_engine.table_sync(source=ori_engine)   # source es OBLIGATORIO para transfer

# copia SCHEMA.TABLA de ori_engine a SCHEMA.TABLA de dest_engine
report = await sync.transfer("SCHEMA", "TABLA", "SCHEMA", "TABLA")

# esquemas/tablas distintos origen -> destino
report = await sync.transfer("ORI", "TABLA_ORI", "DEST", "TABLA_DEST")

# con filtro
report = await sync.transfer("ORI", "TABLA", "DEST", "TABLA",
                             select_sql="SELECT * FROM ORI.TABLA WHERE activo = 1")

print(report.rows_affected, report.used_merge, report.warning)
```

En la fachada síncrona (`BlockingEngine`), usá `merge_sync`/`transfer_sync`
(mismo comportamiento, síncrono).

### Errores comunes

- **`no running event loop`** al conectar/consultar: usaste dos `asyncio.run()`
  separados (uno para `connect` y otro para `fetch_all`). Cada `asyncio.run()`
  crea y cierra su propio event loop, y el engine no sobrevive al cierre.
  Usá un solo `asyncio.run(main())` con todo adentro.
- **`Type "Db2iEngine" is not awaitable`** (warning de Pyright/Pylance): era un
  bug del stub `.pyi`; `connect`/`from_env` ya están tipados como `async def`.

## Síncrono (`BlockingEngine`)

Fachada síncrona en Rust (no un wrapper de `asyncio.run`): misma API que
`Db2iEngine` pero bloqueante, con un runtime tokio propio. Pensada para
call-sites que hoy envuelven `ISeriesConnection` en `asyncio.to_thread(...)`
desde código síncrono (crons de arq, parsers).

```python
from rustodbc.blocking import BlockingEngine

engine = BlockingEngine.from_env("ACME", "PROD")   # o .connect(creds)
rows = engine.fetch_all("SELECT * FROM SCHEMA.TABLA WHERE id = ?", [123])
report = engine.executebatch("INSERT INTO T (a) VALUES (?)", [[1], [2]])
result = engine.call_proc("SCHEMA", "MI_PROC", {"IN": 1})
engine.close()
```

**Streaming síncrono** (reemplaza `iter_dict_chunks`):

```python
for batch in engine.stream("SELECT * FROM SCHEMA.HUGE_TABLE", batch_size=5000):
    for row in batch:
        ...
```

**MERGE síncrono:**

```python
sync = engine.table_sync()
report = sync.merge_sync("SCHEMA", "TABLA", [{"ID": 1, "VALOR": "a"}])
```

**Escritura en paralelo síncrona:**

```python
report = engine.batch_execute("INSERT INTO T (a) VALUES (?)", [[1], [2]], max_workers=4)
report = engine.parallel_execute([("INSERT INTO A (id) VALUES (?)", [[1]]), ("INSERT INTO B (id) VALUES (?)", [[2]])])
```

**Copiar tabla síncrona** (requiere crear el sync con `source`):

```python
sync = dest_engine.table_sync(source=ori_engine)
report = sync.transfer_sync("ORI", "TABLA_ORI", "DEST", "TABLA_DEST")
```

**Regla importante:** `BlockingEngine` levanta `InterfaceError` si se llama
desde un hilo con un **event loop de asyncio corriendo** — para que nadie
termine bloqueando el loop de arq. Si estás dentro de un loop, usá la API
async (`Db2iEngine`).

---

## Requisito de runtime (despliegue)

El wheel de `rustodbc` **no** trae el driver ODBC de IBM. En runtime la
imagen/host necesita:

- El driver *IBM i Access ODBC Driver* (`ibm-iaccess`, repo apt
  `ibmi-acs-1.1.0`) instalado.
- `unixODBC` (Linux) o el subsistema ODBC de Windows (ya presente en
  Windows).

En Linux, el wheel se buildea con `libodbc.so.*` **excluido** explícitamente
(`auditwheel --exclude`) — nunca vendoreado — porque cargar el driver de IBM
contra una versión de `libodbc` distinta a la del sistema puede corromper
buffers `SQLWCHAR`/CCSID. Ver AGENTS.md ss2 y ss9 para el detalle completo.

---

## CI/CD

- `.github/workflows/ci.yml` — en cada push/PR: `cargo fmt` + `clippy
  --all-targets --all-features -- -D warnings` + lint mecánico del GIL, y
  `cargo check` + `maturin build` + smoke import del wheel, en Windows y
  Linux.
- `.github/workflows/release.yml` — `workflow_dispatch` manual: bump de
  versión + tag, build de sdist + wheels. Windows con `maturin-action`;
  Linux (manylinux_2_28 y musllinux_1_2 x86_64) con `cibuildwheel` usando las
  imágenes oficiales de PyPA y `auditwheel --exclude libodbc.so.*`. Crea un
  GitHub Release con todos los assets.
- `.github/workflows/publish.yml` — `workflow_dispatch` manual: baja los
  assets del release y los publica a PyPI (`rustodbc-mi`) vía Trusted
  Publishing (OIDC, sin token de larga vida). Requiere el *trusted publisher*
  configurado una vez en pypi.org para el proyecto `rustodbc-mi` (workflow
  `publish.yml`, environment `pypi`).

No hay wheels de macOS ni de Linux aarch64 hoy: no hay evidencia de que el
driver *IBM i Access ODBC* exista para esas plataformas.

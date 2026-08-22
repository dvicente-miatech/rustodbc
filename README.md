# rustodbc

Motor ODBC async en Rust para IBM DB2 for i (iSeries / AS400), expuesto como
extensión de Python (PyO3 + maturin).

Ver [AGENTS.md](AGENTS.md) para el diseño completo, el estado real de cada
módulo, y el bloqueador de entorno activo (falta el toolchain C++ de MSVC en
la máquina de desarrollo actual para poder compilar).

> **Estado del proyecto:** solo `Credentials`, `EngineOptions`, `load_dotenv`
> y el árbol de excepciones están implementados de verdad hoy. El resto de la
> API (`Db2iEngine`, streaming, `call_proc`, `executebatch`, `TableSync`) está
> diseñado (ver `python/rustodbc/__init__.pyi`) pero **no compilado
> todavía** — se marca explícitamente como tal en cada sección de abajo.

## Instalación

Una vez publicado en PyPI:

```bash
pip install rustodbc-mi
```

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

## Roadmap — diseñado, todavía no implementado

Todo lo que sigue existe como firma en `python/rustodbc/__init__.pyi` (fuente
de verdad del diseño) pero **no tiene código Rust real detrás todavía**
(bloqueado por falta del toolchain C++ en la máquina de desarrollo actual —
ver AGENTS.md ss9). Se documenta acá para que quede claro el rumbo, no para
generar la expectativa de que ya funciona.

```python
# Patrón async (planeado)
engine = await rustodbc.Db2iEngine.from_env("ACME", "PROD")
rows = await engine.fetch_all("SELECT * FROM SCHEMA.TABLE WHERE id = ?", [123])
async for row in engine.stream("SELECT * FROM SCHEMA.HUGE_TABLE"):
    ...
await engine.execute("UPDATE SCHEMA.TABLE SET x = ? WHERE id = ?", [1, 123])
await engine.executebatch(sql, rows)          # cero llamadores medidos hoy
result = await engine.call_proc("SCHEMA", "MI_PROC", {"in_param": 1})

# TableSync por composición, nunca por herencia
sync = dest_engine.table_sync(source=ori_engine)
await sync.merge("SCHEMA", "TABLA", records)

# Fachada sincrona, para call-sites que hoy envuelven pyodbc en to_thread
from rustodbc.blocking import BlockingEngine
engine = BlockingEngine.from_env("ACME", "PROD")
rows = engine.fetch_all(sql, params)
```

`rustodbc.blocking` hoy levanta `NotImplementedError` explícitamente al
importarse — no es un bug, es el estado real documentado.

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

- `.github/workflows/ci.yml` — `cargo fmt`/`clippy`/`build` + `maturin
  develop` + smoke import, en Windows y Linux, en cada push/PR.
- `.github/workflows/release.yml` — `workflow_dispatch` manual: bump de
  versión, tag, build de sdist + wheels (Windows x64, Linux x86_64
  manylinux/musllinux) vía `maturin-action`, y creación de un GitHub Release
  con todos los artifacts adjuntos.
- `.github/workflows/publish.yml` — `workflow_dispatch` manual: descarga los
  assets de un release y los publica a PyPI vía Trusted Publishing (OIDC, sin
  token de larga vida). Requiere configurar el *trusted publisher* una vez en
  pypi.org para este repo.

No hay wheels de macOS ni de Linux aarch64 hoy: no hay evidencia de que el
driver *IBM i Access ODBC* exista para esas plataformas.

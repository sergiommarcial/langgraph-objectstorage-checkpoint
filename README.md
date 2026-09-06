# langgraph-checkpoint-objectstorage

[![CI](https://github.com/sergiommarcial/langgraph-objectstorage-checkpoint/actions/workflows/ci.yml/badge.svg)](https://github.com/sergiommarcial/langgraph-objectstorage-checkpoint/actions/workflows/ci.yml)
[![OS](https://img.shields.io/badge/OS-Linux-blue)](.github/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/langgraph-checkpoint-objectstorage)](https://pypi.org/project/langgraph-checkpoint-objectstorage/)
[![Python versions](https://img.shields.io/pypi/pyversions/langgraph-checkpoint-objectstorage)](https://pypi.org/project/langgraph-checkpoint-objectstorage/)
[![License](https://img.shields.io/github/license/sergiommarcial/langgraph-objectstorage-checkpoint)](LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/langgraph-checkpoint-objectstorage)](https://pypi.org/project/langgraph-checkpoint-objectstorage/)

A [LangGraph](https://github.com/langchain-ai/langgraph) `BaseCheckpointSaver`
that persists checkpoints to local filesystem, Google Cloud Storage, or AWS
S3. One class, backend picked by connection string, nothing to run beyond a
bucket (or a directory).

**Most checkpoint savers make you run a database. This one just needs a
bucket you probably already have.**

## Table of contents

- [Features](#features)
- [Requirements](#requirements)
- [Install](#install)
- [Quickstart](#-quickstart)
- [Examples](#examples)
- [Choosing a backend](#choosing-a-backend)
- [Checkpoint TTL](#-checkpoint-ttl)
- [Compression](#-compression)
- [Encryption](#-encryption)
- [Thread export and import](#-thread-export-and-import)
- [Observability](#-observability)
- [Architecture](#architecture)
- [Architecture decision records](#architecture-decision-records)
- [Runtime type checking](#runtime-type-checking)
- [Logging](#logging)
- [Known limitations](#known-limitations)
- [Performance](#-performance)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Features

- `ObjectStorageSaver.from_conn_string(...)` picks local disk, GCS, or S3
  from the URI scheme. No per-backend subclasses.
- Full sync and async support: every `BaseCheckpointSaver` method, both
  flavors (`get_tuple`/`aget_tuple`, `put`/`aput`, `list`/`alist`,
  `put_writes`/`aput_writes`, `delete_thread`/`adelete_thread`).
- Tested against the official contract with
  [`langgraph-checkpoint-conformance`](https://pypi.org/project/langgraph-checkpoint-conformance/)
  on all three backends, not just hand-written assertions.
- Runtime type checking on the public API via
  [typeguard](https://typeguard.readthedocs.io/) catches wrong-argument-type
  mistakes at the call site.
- Ships `py.typed` for full static type coverage under mypy/pyright.
- No database or extra service required in production, just object storage.

> [!NOTE]
> **Best for:** apps already living in S3/GCS/local disk that don't want a
> database in the loop just for checkpointing, or that need checkpoints to
> land in the same bucket as everything else they store.
>
> **Not for:** workloads needing transactional guarantees across
> checkpoints, or heavy concurrent writes to the *same* thread from
> multiple writers -- see [Known limitations](#known-limitations). For
> those, the official `langgraph-checkpoint-postgres` saver is the better
> fit.

### vs. the official savers

|                          | This saver                      | `-sqlite` / `-postgres` |
|--------------------------|----------------------------------|--------------------------|
| Infra to run             | None -- a bucket or a directory | A database server        |
| Backend                  | Local disk, S3, or GCS           | SQLite or Postgres        |
| `put`/`put_writes` model | Each call writes a new object    | Row inserts               |
| `list(filter=...)`       | Client-side (see [Known limitations](#known-limitations)) | Pushed to SQL |
| Retention                | TTL via bucket lifecycle rules, or `delete_expired()` | Up to you |

If you'd rather not run a database just to remember where a graph left off,
this is that option.

## Requirements

Python 3.11+.

## Install

```bash
pip install langgraph-checkpoint-objectstorage        # local filesystem only
pip install "langgraph-checkpoint-objectstorage[s3]"   # + AWS S3
pip install "langgraph-checkpoint-objectstorage[gcs]"  # + Google Cloud Storage
pip install "langgraph-checkpoint-objectstorage[compression]"  # + zstd codec
pip install "langgraph-checkpoint-objectstorage[observability]"  # + OTel adapter
```

## ⚡ Quickstart

```python
from langgraph.graph import END, START, StateGraph
from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def increment(state: dict) -> dict:
    return {"count": state["count"] + 1}


builder = StateGraph(dict)
builder.add_node("increment", increment)
builder.add_edge(START, "increment")
builder.add_edge("increment", END)

saver = ObjectStorageSaver.from_conn_string("file:///tmp/checkpoints")
graph = builder.compile(checkpointer=saver)

config = {"configurable": {"thread_id": "1"}}
result = graph.invoke({"count": 0}, config)
print(result)  # {"count": 1}

# Checkpoints persisted under the thread survive process restarts.
# Inspect or resume from the same thread_id at any later point:
history = list(graph.get_state_history(config))
```

## Examples

This same quickstart, runnable under three build tools:

- [`examples/pip`](examples/pip): venv + `pip install -r requirements.txt`
- [`examples/uv/filesystem`](examples/uv/filesystem): `uv run main.py`
- [`examples/poetry/filesystem`](examples/poetry/filesystem): `poetry install && poetry run python main.py`

Each installs the package from this repo via a local path dependency
(swap for a normal PyPI dependency once the package is published).

For object storage instead of local disk (a real bucket or a local
emulator, no cloud account needed), there are examples for both S3 and
GCS: multiple independent sessions running sequentially with one resumed
later, and the same pattern again run concurrently via the async API.

- [`examples/uv/s3`](examples/uv/s3) / [`examples/uv/s3-async`](examples/uv/s3-async)
- [`examples/poetry/gcs`](examples/poetry/gcs) / [`examples/poetry/gcs-async`](examples/poetry/gcs-async)

Sequential and concurrent are separate examples rather than one combined
script, see [Known limitations](#known-limitations) for why.

For [compression](#-compression): [`examples/uv/compression`](examples/uv/compression)
writes the same checkpoint with and without it, and prints the size
difference on disk.

For [encryption](#-encryption): [`examples/uv/encryption`](examples/uv/encryption)
writes the same checkpoint with and without it, and checks whether a known
plaintext value shows up in the raw object on disk either way.

For all three together: [`examples/uv/compression-encryption-observability`](examples/uv/compression-encryption-observability)
configures one saver with `compression`, `encryption`, and `on_io` at
once, wiring `otel_on_io_adapter` to a real Prometheus (via `docker
compose`) to show the checkpoint I/O -- byte counts, call counts,
durations -- in an actual dashboard.

## Choosing a backend

Swap the connection string; everything else stays the same.

```python
from langgraph_checkpoint_objectstorage import ObjectStorageSaver

# Local filesystem: handy for development, or single-node deployments
saver = ObjectStorageSaver.from_conn_string("file:///var/lib/my-app/checkpoints")

# Google Cloud Storage
saver = ObjectStorageSaver.from_conn_string("gcs://my-bucket/checkpoints")

# AWS S3
saver = ObjectStorageSaver.from_conn_string("s3://my-bucket/checkpoints")
```

`from_conn_string` forwards extra keyword arguments to the underlying
[fsspec](https://filesystem-spec.readthedocs.io/) filesystem constructor.
Useful for explicit credentials, non-default regions, or S3-compatible
endpoints (MinIO, Cloudflare R2, etc.):

```python
saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    key="...",
    secret="...",
    client_kwargs={"endpoint_url": "https://minio.internal:9000"},
)
```

Credentials otherwise follow each backend's normal resolution: AWS's usual
chain (env vars, `~/.aws/credentials`, instance/task role) for S3,
Application Default Credentials for GCS. There's nothing library-specific
to configure beyond the connection string.

## ⏳ Checkpoint TTL

Pass `ttl` to bound how long checkpoints and writes stick around:

```python
from datetime import timedelta

saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    ttl=timedelta(days=30),
)
```

`ttl=None` (the default) turns TTL off completely: nothing expires on its
own, exactly like before this option existed. Setting `ttl` logs a
one-time `WARNING` on construction as a reminder that the saver itself
never deletes anything -- there's no way for it to check whether a bucket
lifecycle rule actually exists, so it just tells you what to go set up.

**Local filesystem** has no built-in expiry, so you call `delete_expired()`
(or `adelete_expired()`) yourself on a schedule (cron, k8s CronJob, ...):

```python
saver.delete_expired()  # deletes everything older than `ttl`
```

**S3 and GCS** don't need `delete_expired()` at all if you configure a
bucket lifecycle rule against the saver's `root` prefix. The cloud provider
then expires objects on its own schedule (typically once a day), with no
saver code involved. `ttl` still documents the intended age; keeping the
bucket rule's age in sync with it is on you.

S3 lifecycle rule (filtered by `root`, e.g. `"checkpoints/"`):

```json
{
  "Rules": [
    {
      "ID": "expire-checkpoints",
      "Filter": {"Prefix": "checkpoints/"},
      "Status": "Enabled",
      "Expiration": {"Days": 30}
    }
  ]
}
```

GCS lifecycle rule (same idea, via `gsutil` or the console):

```json
{
  "rule": [
    {
      "action": {"type": "Delete"},
      "condition": {"age": 30, "matchesPrefix": ["checkpoints/"]}
    }
  ]
}
```

`delete_expired()`/`adelete_expired()` also work against S3/GCS-backed
savers. They're useful before a lifecycle rule is set up, or when you want
deletion faster than the cloud provider's own cadence.

> [!WARNING]
> **Sharing a bucket with other data?** Always end the lifecycle rule's
> prefix with `/`, exactly as in the examples above. S3's `Filter.Prefix`
> and GCS's `matchesPrefix` do a literal string match with no concept of
> where a path actually ends, so `"Prefix": "checkpoints"` without the
> trailing slash also matches `checkpoints-backup/...` or
> `checkpoints-v2/...`, quietly expiring unrelated data that happens to sit
> next to this saver's `root`. That risk is specific to the lifecycle rule
> you configure by hand, though: `delete_expired()` itself treats `root` as
> a real directory path, not a raw prefix, so it doesn't have this problem.

## 🗜️ Compression

Pass `compression` to shrink checkpoint and write objects before upload:

```python
saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    compression="lzma",
)
# or inline in the connection string:
saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints?compression=lzma"
)
```

`compression="none"` (the default) is byte-identical to every release
before this option existed -- nothing changes unless you opt in. Accepted
values:

- `"none"` -- no compression (default).
- `"zlib"` / `"lzma"` -- standard library, no extra dependency. `lzma`
  compresses smaller but slower than `zlib`.
- `"zstd"` -- faster than both at a comparable or better ratio, but needs
  the `compression` extra
  (`pip install "langgraph-checkpoint-objectstorage[compression]"`).
  Requesting it without the extra installed raises `ImportError`
  immediately at construction, not on the next `put`/`get_tuple`.

Every object records its own codec, so changing `compression` between
deploys of the same application is safe: old objects stay readable under
whichever codec wrote them, new objects use the new one, and a bucket can
mix codecs indefinitely with no migration step.

Worth enabling for threads with large, compressible state (accumulated
message history, JSON-like tool outputs); skip it for small checkpoints or
already-compressed/high-entropy values (embeddings, binary blobs), where
compression buys little and can even net-expand tiny payloads.
Compression runs synchronously on the event loop -- see
[ADR 0003](docs/adr/0003-checkpoint-compression.md) for the tradeoff.
Compressed objects also lose the plain-`cat`/`aws s3 cp` inspectability
uncompressed objects have.

## 🔒 Encryption

Pass `encryption` with a `KeyProvider` implementation to encrypt checkpoint
and write objects with AES-256-GCM before upload:

```python
class MyKeyProvider:
    def get_key(self, thread_id: str, key_id: str | None = None) -> tuple[str, bytes]:
        # key_id=None means "give me the current key to encrypt with";
        # a specific key_id means "resolve exactly this historical key
        # to decrypt with" -- wire up your KMS/Vault/static-key lookup here.
        return "k1", my_32_byte_key

saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    encryption=MyKeyProvider(),
)
```

`encryption=None` (the default) is byte-identical to every release before
this option existed -- nothing changes unless you opt in. Requires the
`encryption` extra
(`pip install "langgraph-checkpoint-objectstorage[encryption]"`).
Requesting it without the extra installed raises `ImportError` immediately
at construction.

Every object records its own `key_id`, so rotating to a new key doesn't
require migrating existing objects -- as long as your `KeyProvider` can
still resolve every `key_id` it has ever issued. A single-key provider
that ignores `key_id` and always returns the same key is a valid, minimal
implementation if you don't need rotation.

The AEAD's associated data is bound to the object's full storage identity:
`thread_id`, `checkpoint_ns`, and `checkpoint_id` for a checkpoint, plus
`task_id` and the write index for a pending write. Move an object to a
different thread, namespace, checkpoint, task, or write index and it
fails to decrypt, instead of silently decrypting under whatever key that
new path resolves to. Once `encryption` is configured, reading an object
that was never encrypted is also an error, not a silent pass-through.

> [!NOTE]
> Encryption here is symmetric AEAD (AES-256-GCM) only -- there's no
> direct asymmetric (RSA/ECIES) payload encryption, since no real system
> encrypts bulk data that way. Asymmetric key *management* (an RSA- or
> KMS-wrapped data key) is still fully supported: it's entirely contained
> inside your `KeyProvider` implementation, which the library treats as
> opaque.

> [!WARNING]
> This only encrypts payload bytes. Object paths still encode `thread_id`,
> `checkpoint_ns`, `checkpoint_id`, and task/channel names in plaintext --
> anyone with bucket read access still sees thread activity patterns and
> checkpoint cadence. Losing your `KeyProvider`'s ability to resolve a
> `key_id` makes every object written under that key permanently
> unreadable; there's no recovery path inside this library. See
> [ADR 0004](docs/adr/0004-checkpoint-encryption.md) for the full design
> and tradeoffs.

## 📤 Thread export and import

`export_thread`/`aexport_thread` pack a thread's full checkpoint and write
history -- across every `checkpoint_ns` -- into a portable tar archive.
`import_thread`/`aimport_thread` restore one back, optionally under a new
thread_id or into a different backend entirely (local disk, S3, GCS):

```python
archive = saver.export_thread("thread-1")

dest_saver = ObjectStorageSaver.from_conn_string("gcs://my-bucket/checkpoints")
dest_saver.import_thread(archive)
```

The archive is opaque bytes: whatever `compression`/`encryption` produced
the underlying objects stays exactly as written, so reading the result
back requires the same `KeyProvider` the source was encrypted under, if
any.

By default `import_thread` restores under the same thread_id the archive
was exported from. Pass `dest_thread_id` to rename during import -- but
never do this for an encrypted archive: AES-256-GCM's associated data is
bound to `thread_id` (see [Encryption](#-encryption)), so a renamed,
encrypted checkpoint fails to decrypt permanently, with no recovery path.

By default, importing onto an existing key raises without writing
anything; pass `overwrite=True` to replace it.

> [!WARNING]
> The whole archive is built and held in memory as `bytes` on both ends --
> fine for typical thread histories, a real amount of memory for a very
> large one. See [ADR 0007](docs/adr/0007-thread-export-import.md) for the
> full design, including why this only moves data between
> `ObjectStorageSaver` backends (local disk/S3/GCS), not to or from a
> different `BaseCheckpointSaver` implementation like the official
> Postgres/SQLite savers.

## 📈 Observability

Pass `on_io` to see the cost of every backend call (`find`/`cat`/`pipe`/
`exists`/`rm`) as it happens -- key, byte counts, duration, and any error:

```python
def log_io(event):
    print(f"{event.op} {event.key} {event.duration_ms:.1f}ms err={event.error}")

saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    on_io=log_io,
)
```

`on_io=None` (the default) is byte-identical to every release before this
option existed. `on_io` can be a plain function or an `async def` -- an
awaitable return value is awaited on the same coroutine/thread as the I/O
call it observed. It runs synchronously in the I/O path, so keep it fast;
an exception it raises (or a cancelled call's `asyncio.CancelledError`,
which `on_io` still sees) is logged and swallowed rather than breaking the
real I/O call.

> [!NOTE]
> On local disk, every call on a saver instance shares one persistent
> background loop/thread (see [ADR 0006](docs/adr/0006-persistent-event-loop.md)),
> so a slow `on_io` there stalls every other concurrent call on that
> instance, not just the one it's timing.

Requires no dependency by itself. For OpenTelemetry, `otel_on_io_adapter`
ships one ready-made adapter (requires the `observability` extra):

```python
from opentelemetry import trace
from langgraph_checkpoint_objectstorage import otel_on_io_adapter

tracer = trace.get_tracer("my-app")
saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    on_io=otel_on_io_adapter(tracer),
)
```

Pass `meter=` too to also record an `objectstorage.io.duration` histogram,
an `objectstorage.io.bytes` counter, and an `objectstorage.io.count`
histogram (keys scanned per `find`/`list`/TTL sweep -- the number behind
this saver's one documented O(n) cost), all tagged by `op`:

```python
saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    on_io=otel_on_io_adapter(tracer, meter=meter_provider.get_meter("my-app")),
)
```

A `FileNotFoundError` -- this saver's normal "nothing here yet" signal for
an empty thread's first read, or deleting an already-gone thread -- is
recorded on the span for context but never flips its status to `ERROR` or
the metrics' `error` attribute to `true`; only a real failure does. A
cancelled call (e.g. a timed-out run) still reaches `on_io` too, but the
adapter's own span/metric work for it runs detached from the
cancellation's unwind, so a slow `on_io` can't turn "cancel this" into
"cancel this, eventually."

Getting these spans/metrics into a specific backend is exporter
configuration on your side -- the adapter above is backend-neutral. A few
common setups:

```python
# Datadog -- OTLP intake via the Datadog Agent (default gRPC port 4317)
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter

tracer_provider = TracerProvider()
tracer_provider.add_span_processor(
    BatchSpanProcessor(OTLPSpanExporter(endpoint="http://localhost:4317"))
)
meter_provider = MeterProvider(metric_readers=[
    PeriodicExportingMetricReader(OTLPMetricExporter(endpoint="http://localhost:4317"))
])
saver = ObjectStorageSaver.from_conn_string(
    "s3://my-bucket/checkpoints",
    on_io=otel_on_io_adapter(
        tracer_provider.get_tracer("objectstorage"),
        meter=meter_provider.get_meter("objectstorage"),
    ),
)
```

```python
# Prometheus -- pull-based, metrics only (spans need a separate tracing
# backend such as Jaeger or Tempo -- Prometheus doesn't ingest traces)
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from prometheus_client import start_http_server

start_http_server(9464)  # scrape http://localhost:9464/metrics
meter_provider = MeterProvider(metric_readers=[PrometheusMetricReader()])
```

```python
# Dynatrace -- OTLP endpoint on your environment, with an API token
OTLPSpanExporter(
    endpoint="https://<environment-id>.live.dynatrace.com/api/v2/otlp/v1/traces",
    headers={"Authorization": "Api-Token <token>"},
)
```

> [!NOTE]
> `IOEvent.count` (for `find`) and `.nbytes` (for `cat`/`pipe`) are exactly
> the numbers behind the O(n) costs called out in
> [Known limitations](#known-limitations) -- `on_io` is how you see them
> in your own deployment instead of hitting them blind in production. See
> [ADR 0008](docs/adr/0008-io-observability.md) for the full design.

## Architecture

Business logic (key layout, filtering, ordering, idempotency) is written
once, as async methods. The public sync API is a thin `asyncio.run(...)`
wrapper around that same async core, not a second implementation, so
there's a single source of truth per operation instead of sync and async
code drifting apart. An I/O bridge picks native async calls when the
backend supports them (`s3fs`, `gcsfs`) and falls back to
`asyncio.to_thread` when it doesn't (local disk):

```mermaid
flowchart TD
    App["Your application<br/>(graph.invoke / ainvoke)"]

    subgraph PublicAPI["Public API (BaseCheckpointSaver contract)"]
        Sync["put / get_tuple / list /<br/>put_writes / delete_thread"]
        Async["aput / aget_tuple / alist /<br/>aput_writes / adelete_thread"]
    end

    Core["Async core<br/>(business logic, written once)"]
    Bridge["I/O bridge<br/>_cat / _pipe / _find / _exists / _rm"]
    Native["fsspec async-native<br/>(s3fs, gcsfs)"]
    Threaded["asyncio.to_thread<br/>(LocalFileSystem)"]
    Backend[("Local disk / S3 / GCS")]

    App --> Sync
    App --> Async
    Sync -->|"asyncio.run(...)<br/>thin wrapper, not a<br/>second implementation"| Core
    Async --> Core
    Core --> Bridge
    Bridge -->|backend supports async| Native --> Backend
    Bridge -->|no native async| Threaded --> Backend
```

Each checkpoint and each write becomes its own object: no read-modify-write
on existing keys, so concurrent writers on different threads never race,
and a `put` or `put_writes` call is always a single write:

```mermaid
flowchart TD
    Root["{root}"] --> Thread["{thread_id}/"]
    Thread --> NS["{checkpoint_ns}/"]
    NS --> CkptDir["checkpoints/"]
    NS --> WriteDir["writes/"]
    CkptDir --> Ckpt["{checkpoint_id}.msgpack<br/>checkpoint + metadata + parent_checkpoint_id"]
    WriteDir --> WCkpt["{checkpoint_id}/"]
    WCkpt --> WTask["{task_id}/"]
    WTask --> WIdx["{idx}.msgpack<br/>task_id + idx + channel + value"]
```

Key technical decisions this reflects:

- `checkpoint_id` is LangGraph's own [uuid6](https://github.com/langchain-ai/langgraph/blob/main/libs/checkpoint/langgraph/checkpoint/base/id.py),
  already time-sortable, so `list()`'s newest-first ordering falls out of a
  plain key sort, with no secondary index to keep in sync.
- Checkpoints and metadata are serialized through LangGraph's own
  `JsonPlusSerializer` and treated as opaque: never hand-extracted field by
  field, since real LangGraph objects carry fields their own TypedDicts
  don't declare (see [Runtime type checking](#runtime-type-checking)).
- `put_writes`' overwrite-vs-ignore split ("first write wins" for regular
  channels, always-replace for the control channels `ERROR`/`SCHEDULED`/
  `INTERRUPT`/`RESUME`) mirrors the official sqlite/postgres savers exactly.
- `list(filter=...)` is deliberately client-side, not an oversight: object
  storage has no query engine to push a filter into. See
  [Known limitations](#known-limitations).

## Architecture decision records

Design proposals and decisions that change or extend the architecture
above live under [`docs/adr/`](docs/adr/), not in this README:

- [`0001-checkpoint-ttl.md`](docs/adr/0001-checkpoint-ttl.md) on why TTL enforcement is
backend-specific instead of one mechanism for all three.
- [`0002-slatedb.md`](docs/adr/0002-slatedb.md) --
  proposal for an opt-in SlateDB-backed storage mode to bound
  `get_tuple(latest)`/`list()` cost on threads with very large checkpoint
  histories (see [Known limitations](#known-limitations)). Status:
  proposed, not implemented.
- [`0003-checkpoint-compression.md`](docs/adr/0003-checkpoint-compression.md)
  on the pluggable codec registry and wire format behind the
  `compression` option.
- [`0004-checkpoint-encryption.md`](docs/adr/0004-checkpoint-encryption.md)
  on the `KeyProvider` protocol and AES-256-GCM wire format behind the
  `encryption` option.
- [`0005-performance-benchmarks.md`](docs/adr/0005-performance-benchmarks.md)
  on the pytest-benchmark suite, its backend/envelope/scale matrix, and why
  CI regression gating and load testing are deferred.
- [`0006-persistent-event-loop.md`](docs/adr/0006-persistent-event-loop.md)
  on the persistent background event loop behind local-disk sync calls,
  found via that benchmark suite's profiling.
- [`0007-thread-export-import.md`](docs/adr/0007-thread-export-import.md)
  on the `export_thread`/`import_thread` design: raw-bytes tar archives,
  full-thread-only scope, and why renaming an encrypted thread on import
  is unsafe.
- [`0008-io-observability.md`](docs/adr/0008-io-observability.md) on the
  `on_io` callback hook and why OTel support is one adapter over that
  callback rather than spans built directly into the saver.

## Runtime type checking

Public methods are decorated with [typeguard](https://typeguard.readthedocs.io/)
and raise `typeguard.TypeCheckError` on a call with the wrong argument
types (e.g. a non-string `thread_id`, a `writes` argument that isn't a
sequence of `(channel, value)` pairs). This catches integration mistakes
at the call site instead of letting them corrupt stored data silently.

`config`, `checkpoint`, and `metadata` arguments are intentionally *not*
strictly checked against LangGraph's `RunnableConfig`/`Checkpoint`/
`CheckpointMetadata` TypedDicts: real LangGraph objects don't match those
TypedDicts exactly (a real `RunnableConfig`'s `metadata` field is a
`collections.ChainMap`, not a plain `dict`; real checkpoints carry fields
like the legacy `pending_sends` key that isn't declared at all), and strict
checking would reject every real invocation. The same logic applies to
`get_tuple`/`list`'s return value, which also isn't runtime-checked. Other
arguments (`thread_id`, `task_id`, `writes`, `limit`, ...) are checked
normally.

## Logging

Uses standard `logging` under the logger name
`langgraph_checkpoint_objectstorage`. No handlers are configured, so it
stays silent until your application's logging config says otherwise.

For quick debugging, set `LANGGRAPH_CHECKPOINT_OBJECTSTORAGE_LOG_LEVEL=DEBUG`
before constructing an `ObjectStorageSaver`. It emits one DEBUG line per
storage read/write/list with the key or prefix touched.

## Known limitations

- `list(filter=...)` is client-side: every checkpoint in the thread/namespace
  is fetched and filtered in Python, since object storage has no query
  engine to push the filter into. Fine for typical thread histories (dozens
  to low hundreds of checkpoints); a very long-running thread's `list` calls
  will get proportionally slower.
- `get_tuple` without an explicit `checkpoint_id` (resolving "latest") pays
  the same cost: it lists every checkpoint key under the thread/namespace to
  find the newest one. Since this runs on every resume of an existing
  thread, a thread with a very large checkpoint history will see resume
  latency grow with checkpoint count, not just `list()` calls. See
  [`docs/adr/0002-slatedb.md`](docs/adr/0002-slatedb.md)
  for a proposed fix (currently a design proposal, not yet implemented).
- TTL is per-object age, not per-checkpoint-chain: a checkpoint's `writes`
  objects are added later (via `put_writes`) and age out on their own
  clock, so they can expire slightly before or after the checkpoint they
  belong to. There's no read-time filtering either: an object past its TTL
  is still returned by `get_tuple`/`list` until something actually deletes
  it (a lifecycle rule run, or a `delete_expired()` call). See
  [Checkpoint TTL](#checkpoint-ttl).
- Two writers on the *same* `thread_id` writing concurrently can race, at
  the same guarantee level as the official sqlite saver (last write "wins"
  by whichever checkpoint_id sorts last, not by wall-clock order under
  clock skew). Concurrent writers on different threads never race.
- The sync `list()` eagerly collects all matching checkpoints before
  yielding the first one (it wraps the async implementation via
  `asyncio.run`, which can't stream lazily). Use `alist()` from async code
  if you need true streaming.
- `export_thread`/`import_thread` buffer a full thread's history as
  `bytes` in memory on both ends; there's no streaming variant yet.
  Renaming an encrypted thread via `dest_thread_id` also permanently
  breaks decryption -- see
  [Thread export and import](#-thread-export-and-import).
- Two visually-identical `thread_id`/`checkpoint_id`/`checkpoint_ns`/
  `task_id` values that use different Unicode normalization forms of the
  same characters (e.g. a precomposed "é" vs. the same letter built from
  combining characters) aren't detected as distinct or merged -- what
  happens depends entirely on the backend's filesystem. This saver does
  no normalization itself. Not reproducible on this project's tested
  targets: Linux local disk (ext4 and friends don't normalize) or S3/GCS
  (both store the exact byte sequence as the key, with no normalization
  at all). Only a concern on a local-disk deployment using a
  normalizing filesystem (notably macOS's HFS+/APFS), where two such
  values can silently resolve to the same on-disk path.

> [!WARNING]
> `thread_id`, `checkpoint_id`, and `task_id` must each be a single,
> non-empty path segment matching letters, digits, and `. : - _`; not `.`
> or `..` on their own. `checkpoint_ns` follows the same rule except
> `checkpoint_ns=""` (the default namespace) is always valid. This
> saver's key layout joins these values with `/` as a path separator, so
> a value outside this set could collide with, or (for a value containing
> `/` or `\` or equal to `..`) write outside, a different
> thread/checkpoint/task's storage keys -- or, for `delete_thread`,
> recursively delete outside `root` entirely. `put`/`get_tuple`/`list`/
> `put_writes`/`delete_thread`, and `export_thread`/`import_thread`, all
> raise `ValueError` if any of these values is invalid.

> [!WARNING]
> Don't mix sync and async calls on the *same* `ObjectStorageSaver`
> instance against S3 or GCS. Sync calls run on a persistent background
> loop the underlying filesystem maintains; async calls run on whichever
> loop the caller provides. The aiohttp session those backends use can
> only belong to one loop at a time, so alternating between the two on one
> instance breaks with `RuntimeError: ... attached to a different loop`.
> Build a separate saver instance per usage style instead (see
> `examples/uv/s3` vs `examples/uv/s3-async`, or `examples/poetry/gcs` vs
> `examples/poetry/gcs-async`). Local filesystem isn't affected: it has no
> persistent session to misalign.

## 📊 Performance

<!-- BENCHMARK-RESULTS:START -->
| Operation | Backend | Dimension | Mean (µs) | StdDev (µs) | Ops/sec | Notes |
|---|---|---|---|---|---|---|
| delete_thread | local | history_size=10 | 559.92 | 226.21 | 1786.0 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| delete_thread | local | history_size=100 | 2521.66 | 99.80 | 396.6 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| delete_thread | local | history_size=1000 | 29005.59 | 447.60 | 34.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| delete_thread | s3 | history_size=10 | 6120.35 | 4038.27 | 163.4 |  |
| delete_thread | s3 | history_size=100 | 16373.48 | 3467.22 | 61.1 |  |
| delete_thread | s3 | history_size=1000 | 121748.37 | 3351.63 | 8.2 |  |
| put | local | envelope=plain | 177.39 | 13.63 | 5637.3 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=compression | 181.63 | 14.71 | 5505.7 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=encryption | 209.81 | 12.87 | 4766.2 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | s3 | envelope=plain | 2705.39 | 120.74 | 369.6 |  |
| put | s3 | envelope=compression | 2728.44 | 252.21 | 366.5 |  |
| put | s3 | envelope=encryption | 2691.06 | 141.09 | 371.6 |  |
| get_tuple | local | envelope=plain | 189.41 | 12.05 | 5279.7 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=compression | 194.33 | 11.81 | 5145.8 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=encryption | 212.57 | 17.79 | 4704.4 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | s3 | envelope=plain | 6758.82 | 347.64 | 148.0 |  |
| get_tuple | s3 | envelope=compression | 6740.25 | 238.77 | 148.4 |  |
| get_tuple | s3 | envelope=encryption | 6771.86 | 814.19 | 147.7 |  |
| put_writes | local | envelope=plain | 155.88 | 11.07 | 6415.3 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put_writes | local | envelope=compression | 162.61 | 10.98 | 6149.8 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put_writes | local | envelope=encryption | 187.51 | 12.54 | 5333.2 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put_writes | s3 | envelope=plain | 2613.80 | 395.03 | 382.6 |  |
| put_writes | s3 | envelope=compression | 2548.17 | 316.59 | 392.4 |  |
| put_writes | s3 | envelope=encryption | 2703.46 | 214.77 | 369.9 |  |
| put | local | envelope=plain, payload_size=small | 179.68 | 15.42 | 5565.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=plain, payload_size=medium | 176.13 | 12.32 | 5677.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=plain, payload_size=large | 292.43 | 887.37 | 3419.6 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=compression, payload_size=small | 178.27 | 11.81 | 5609.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=compression, payload_size=medium | 184.87 | 12.69 | 5409.1 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=compression, payload_size=large | 338.30 | 13.38 | 2955.9 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=encryption, payload_size=small | 212.11 | 14.49 | 4714.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=encryption, payload_size=medium | 213.06 | 14.03 | 4693.6 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | local | envelope=encryption, payload_size=large | 437.49 | 333.20 | 2285.8 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| put | s3 | envelope=plain, payload_size=small | 2812.47 | 570.42 | 355.6 |  |
| put | s3 | envelope=plain, payload_size=medium | 6495.78 | 18988.01 | 153.9 |  |
| put | s3 | envelope=plain, payload_size=large | 6312.02 | 284.38 | 158.4 |  |
| put | s3 | envelope=compression, payload_size=small | 2754.15 | 203.13 | 363.1 |  |
| put | s3 | envelope=compression, payload_size=medium | 2718.91 | 104.48 | 367.8 |  |
| put | s3 | envelope=compression, payload_size=large | 2773.08 | 134.38 | 360.6 |  |
| put | s3 | envelope=encryption, payload_size=small | 2666.41 | 101.66 | 375.0 |  |
| put | s3 | envelope=encryption, payload_size=medium | 2743.14 | 148.88 | 364.5 |  |
| put | s3 | envelope=encryption, payload_size=large | 6347.83 | 142.49 | 157.5 |  |
| get_tuple | local | envelope=plain, payload_size=small | 184.71 | 12.39 | 5413.8 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=plain, payload_size=medium | 186.41 | 12.09 | 5364.6 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=plain, payload_size=large | 251.98 | 12.71 | 3968.6 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=compression, payload_size=small | 192.06 | 10.20 | 5206.6 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=compression, payload_size=medium | 192.72 | 18.71 | 5188.9 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=compression, payload_size=large | 291.39 | 38.19 | 3431.8 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=encryption, payload_size=small | 218.18 | 12.44 | 4583.3 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=encryption, payload_size=medium | 218.91 | 14.83 | 4568.1 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | local | envelope=encryption, payload_size=large | 425.35 | 19.60 | 2351.0 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple | s3 | envelope=plain, payload_size=small | 8699.71 | 14578.88 | 114.9 |  |
| get_tuple | s3 | envelope=plain, payload_size=medium | 6867.11 | 1394.81 | 145.6 |  |
| get_tuple | s3 | envelope=plain, payload_size=large | 8837.81 | 190.81 | 113.2 |  |
| get_tuple | s3 | envelope=compression, payload_size=small | 6909.49 | 1154.11 | 144.7 |  |
| get_tuple | s3 | envelope=compression, payload_size=medium | 6737.41 | 202.66 | 148.4 |  |
| get_tuple | s3 | envelope=compression, payload_size=large | 6909.12 | 1287.43 | 144.7 |  |
| get_tuple | s3 | envelope=encryption, payload_size=small | 6809.04 | 299.76 | 146.9 |  |
| get_tuple | s3 | envelope=encryption, payload_size=medium | 6852.16 | 1241.48 | 145.9 |  |
| get_tuple | s3 | envelope=encryption, payload_size=large | 9242.33 | 1274.48 | 108.2 |  |
| get_tuple_latest | local | history_size=10 | 211.71 | 13.38 | 4723.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple_latest | local | history_size=100 | 481.08 | 21.31 | 2078.7 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple_latest | local | history_size=1000 | 3028.68 | 104.34 | 330.2 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| get_tuple_latest | s3 | history_size=10 | 7738.79 | 969.60 | 129.2 |  |
| get_tuple_latest | s3 | history_size=100 | 18000.74 | 15928.65 | 55.6 |  |
| get_tuple_latest | s3 | history_size=1000 | 103855.47 | 32500.78 | 9.6 |  |
| list_filter | local | history_size=10 | 691.00 | 77.47 | 1447.2 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| list_filter | local | history_size=100 | 5379.64 | 213.17 | 185.9 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| list_filter | local | history_size=1000 | 54143.63 | 1162.95 | 18.5 | [ADR 0006](docs/adr/0006-persistent-event-loop.md) |
| list_filter | s3 | history_size=10 | 21359.44 | 1165.71 | 46.8 |  |
| list_filter | s3 | history_size=100 | 151895.43 | 3198.31 | 6.6 |  |
| list_filter | s3 | history_size=1000 | 1477100.40 | 55335.15 | 0.7 |  |

_Measured on 2026-09-06, on maintainer hardware against local disk and an in-process moto S3 emulator -- a relative comparison across operations/backends/envelopes, not an absolute production guarantee._
<!-- BENCHMARK-RESULTS:END -->

Each row is one operation, run repeatedly under one condition. `Dimension`
says what's varied: `envelope=` is the compression/encryption setting
(see [Compression](#-compression), [Encryption](#-encryption)),
`history_size=` is how many checkpoints already existed in the thread
being read from, written to, or deleted. A bigger history makes
`delete_thread` and the two O(n) costs above slower; it doesn't affect a
plain `put`/`get_tuple`/`put_writes` on a single checkpoint.

`Mean` and `StdDev` are the average time per call and how much that time
varied across runs, in microseconds (1,000 µs = 1 ms) -- a StdDev that's
large relative to the mean means the operation's cost is inconsistent,
not just slow. `Ops/sec` is the same mean turned into "calls per second"
(1,000,000 / mean µs), which is often easier to compare across rows at a
glance.

`s3` rows are against an in-process moto emulator, not real AWS S3. Fine
for comparing operations, backends, and envelopes against each other; not
a real-network latency estimate.

`Notes` links to the ADR responsible for a row's numbers, when one
applies. Every `local` row currently points to
[ADR 0006](docs/adr/0006-persistent-event-loop.md): all local-disk sync
calls share the same persistent background event loop it introduced, so
that one change touches every operation on that backend, not just a
specific one.

Benchmarks cover the hot path (`put`/`get_tuple`/`put_writes`), the
documented O(n) costs above (`list(filter=...)`, `get_tuple(latest)`) at a
few thread-history sizes, payload-size scaling, and `delete_thread` at
scale, across local disk and an in-process moto S3 emulator, with plain,
compressed, and encrypted envelopes. See
[`docs/adr/0005-performance-benchmarks.md`](docs/adr/0005-performance-benchmarks.md)
for the full design. Run them yourself with `make bench` (or
`make bench-compare` / `make bench-report` -- see
[Development](#development)).

## Development

```bash
make install        # sync deps into an isolated .venv (installs uv if missing)
make lint             # black --check, pyflakes, bandit, vulture -- runs before test/build
make format          # apply black formatting in place
make test            # full suite -- docker-compose integration tests auto-skip if not up
make test-unit        # tests/unit only -- no external services, fast
make test-integration  # tests/integration -- starts docker-compose emulators first
make bench           # run performance benchmarks (local + in-process moto S3)
make bench-compare   # compare the last two autosaved benchmark runs
make bench-report    # regenerate the Performance table above + tests/benchmark/report.html
```

`test`/`test-unit`/`test-integration`/`build` all run `lint` first, so a
formatting or static-analysis failure blocks the run rather than
surfacing only after tests pass. `bandit`/`vulture` are scoped to `src/`
only: `bandit` flags every pytest `assert` and the tests' intentionally
fake credentials, and `vulture` can't see that `ObjectStorageSaver`'s
public methods are called by library consumers rather than this codebase
(see `vulture_whitelist.py`).

Or without `make`, directly via `uv`:

```bash
uv sync --all-extras --group dev
uv run pytest
```

Test layout: `tests/unit/` exercises internal modules (key layout,
serialization, the saver's core logic, logging, type checking) with no
external service. `tests/integration/` validates the full
`BaseCheckpointSaver` contract via
[`langgraph-checkpoint-conformance`](https://pypi.org/project/langgraph-checkpoint-conformance/)
against real backends: local disk, an in-process moto server for S3, and
(via `docker-compose.yaml`) `fake-gcs-server`/`moto-server` containers for a
full local GCS/S3 round-trip with no cloud account needed. A gated test
against a real GCS bucket runs only when `GCS_TEST_BUCKET` is set.

```bash
make compose-up      # start local S3/GCS emulators
make test-integration
make compose-down     # stop them when done
```

## Contributing

Issues and PRs welcome. Before opening one, `make test` should pass
(`make test-integration` too, if your change touches backend I/O). CI runs
lint, unit tests (Python 3.11/3.12/3.13), and integration tests against
docker-compose emulators on every push and PR.

Add an entry under `## [Unreleased]` in [`CHANGELOG.md`](CHANGELOG.md) for
any user-facing change. On merge to `main`, CI moves that section into a
new dated version automatically (see the `release` job in
`.github/workflows/ci.yml`). An empty `[Unreleased]` just gets a generic
placeholder line instead, so it's worth taking the extra minute.

### Releasing

The `release` job only bumps `pyproject.toml` and `CHANGELOG.md` and pushes
that commit to `main`; it doesn't tag or publish anything. Publishing to
PyPI is a manual step, since it's the one part of this pipeline that isn't
reversible:

```bash
git pull origin main
git tag -a vX.Y.Z -m vX.Y.Z
git push origin vX.Y.Z
```

Pushing that tag triggers `.github/workflows/publish.yml`, which builds
the package and publishes it to PyPI via trusted publishing (no token
needed). Create the GitHub release from the same tag, e.g.:

```bash
gh release create vX.Y.Z --title vX.Y.Z --generate-notes
```

## License

[MIT](LICENSE).

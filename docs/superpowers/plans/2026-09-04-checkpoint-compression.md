# Checkpoint Compression Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an opt-in `compression` option (`"none"` default, `"zlib"`,
`"lzma"`, `"zstd"`) to `ObjectStorageSaver` that compresses each checkpoint
and write object before upload, fully backward-compatible with every object
already on disk.

**Architecture:** `envelope.py` gains a small `Codec` protocol + registry
and a `_wrap`/`_unwrap` pair that nest the existing packed msgpack dict
inside one more self-describing msgpack wrapper (`{"codec": ..., "payload":
...}`) only when compression is requested. `saver.py` resolves and validates
the requested codec once at construction time and threads it through
`pack_checkpoint`/`pack_write`; unpacking never needs the codec name because
every compressed object carries its own. `keys.py` is untouched.

**Tech Stack:** Python 3.11+, stdlib `zlib`/`lzma`, optional `zstandard`
(new `compression` extra), `ormsgpack`, `pytest`/`pytest-asyncio`,
`langgraph-checkpoint-conformance`.

**Spec:** [`docs/adr/0003-checkpoint-compression.md`](../../adr/0003-checkpoint-compression.md)

## Global Constraints

- `compression="none"` (the default) must produce byte-for-byte identical
  output to today's uncompressed format — this is an explicit opt-in, never
  a new default.
- Core `dependencies` stay backend-agnostic; `zstd` support is gated behind
  a new `compression` extra (pulls in `zstandard`), same pattern as `s3`/
  `gcs`. Requesting `compression="zstd"` without the extra installed raises
  `ImportError` at construction time, not inside a later `put`/`get_tuple`.
- Reads always decode using the `"codec"` field recorded in the object
  itself, never the reading saver's own configured `compression` — a bucket
  can mix codecs across objects with no migration step, and an unrecognized
  codec id on read fails fast naming the unknown codec.
- Every task's tests must keep `report.passed_all_base()` green for the
  local conformance suite, with compression on and off (see ADR's
  Constraint section).
- Compression runs inline/synchronously on the event loop — no
  `asyncio.to_thread` offload (see ADR's "Event-loop blocking on large
  payloads" section). Don't add one.
- No size-threshold skip for tiny payloads — explicitly out of scope per
  the ADR's Follow-up section.

---

### Task 1: Codec registry + compression extra in `envelope.py`

**Files:**
- Modify: `pyproject.toml` (add `compression` extra)
- Modify: `src/langgraph_checkpoint_objectstorage/envelope.py` (full rewrite)
- Test: `tests/unit/test_envelope.py`

**Interfaces:**
- Produces: `envelope.resolve_codec(compression: str) -> str | None` — `"none"` → `None`; a known codec name → itself; an unrecognized name → raises `ValueError`; a recognized-but-not-installed name (`"zstd"` without the extra) → raises `ImportError`. Later tasks call this once at `ObjectStorageSaver` construction.
- Produces: `envelope.pack_checkpoint(checkpoint, metadata, parent_checkpoint_id, codec_name: str | None = None) -> bytes` and `envelope.pack_write(task_id, idx, channel, value, codec_name: str | None = None) -> bytes` — existing 3-arg/4-arg call sites keep working unchanged since `codec_name` defaults to `None` (today's uncompressed behavior).
- Produces: `envelope.unpack_checkpoint(data: bytes)` and `envelope.unpack_write(data: bytes)` — unchanged signatures, now self-describing (no codec argument needed to read).
- Produces (test-only surface, used by Task 2's error-path test): `envelope._CODECS: dict[str, Codec]` — a module-level dict, poppable via `monkeypatch.delitem` to simulate "extra not installed" without actually uninstalling `zstandard`.

- [ ] **Step 1: Add the `compression` extra to `pyproject.toml`**

Edit `pyproject.toml`'s `[project.optional-dependencies]` table:

```toml
[project.optional-dependencies]
s3 = ["s3fs>=2024.10.0"]
gcs = ["gcsfs>=2024.10.0"]
compression = ["zstandard>=0.22"]
```

- [ ] **Step 2: Sync the environment so `zstandard` is installed**

Run: `uv sync --all-extras --group dev`
Expected: completes with `zstandard` installed (dev installs use
`--all-extras`, which now includes `compression`).

- [ ] **Step 3: Write the failing tests for the codec registry and wrap/unwrap round trip**

Add to `tests/unit/test_envelope.py` (new imports at top of file: add
`import pytest` and `import ormsgpack`):

```python
import ormsgpack
import pytest

from langgraph_checkpoint_objectstorage import envelope


def test_resolve_codec_none_returns_none():
    assert envelope.resolve_codec("none") is None


@pytest.mark.parametrize("name", ["zlib", "lzma", "zstd"])
def test_resolve_codec_known_name_returns_itself(name):
    assert envelope.resolve_codec(name) == name


def test_resolve_codec_unknown_name_raises_value_error():
    with pytest.raises(ValueError, match="bogus"):
        envelope.resolve_codec("bogus")


def test_resolve_codec_zstd_without_extra_raises_import_error(monkeypatch):
    monkeypatch.delitem(envelope._CODECS, "zstd", raising=False)
    with pytest.raises(ImportError, match="zstd"):
        envelope.resolve_codec("zstd")


@pytest.mark.parametrize("codec_name", ["zlib", "lzma", "zstd"])
def test_pack_unpack_checkpoint_round_trip_with_compression(codec_name):
    checkpoint = {
        "v": 1,
        "id": "ckpt-1",
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {"k": "v" * 1000},
        "channel_versions": {"k": 1},
        "versions_seen": {"node": {"k": 1}},
    }
    metadata = {"source": "input", "step": 0, "parents": {}}
    data = envelope.pack_checkpoint(checkpoint, metadata, None, codec_name=codec_name)
    out_checkpoint, out_metadata, parent_id = envelope.unpack_checkpoint(data)
    assert out_checkpoint == checkpoint
    assert out_metadata == metadata
    assert parent_id is None


def test_pack_checkpoint_none_codec_matches_legacy_uncompressed_shape():
    data = envelope.pack_checkpoint({"v": 1, "id": "c1"}, {"step": 0}, None)
    obj = ormsgpack.unpackb(data)
    assert set(obj.keys()) == {"checkpoint", "metadata", "parent_checkpoint_id"}


def test_unpack_checkpoint_reads_legacy_object_with_no_wrapper():
    # Simulates an object written before compression existed: no "codec"
    # wrapper at all, just the inner packed dict directly.
    ck_type, ck_bytes = envelope._serde.dumps_typed({"v": 1, "id": "c1"})
    md_type, md_bytes = envelope._serde.dumps_typed({"step": 0})
    legacy = ormsgpack.packb(
        {
            "checkpoint": [ck_type, ck_bytes],
            "metadata": [md_type, md_bytes],
            "parent_checkpoint_id": None,
        }
    )
    checkpoint, metadata, parent_id = envelope.unpack_checkpoint(legacy)
    assert checkpoint == {"v": 1, "id": "c1"}
    assert metadata == {"step": 0}
    assert parent_id is None


def test_unpack_checkpoint_rejects_unknown_codec():
    bogus = ormsgpack.packb({"codec": "brotli", "payload": b"whatever"})
    with pytest.raises(ValueError, match="brotli"):
        envelope.unpack_checkpoint(bogus)


@pytest.mark.parametrize("codec_name", ["zlib", "lzma", "zstd"])
def test_pack_unpack_write_round_trip_with_compression(codec_name):
    data = envelope.pack_write(
        "task-1", 0, "my_channel", {"nested": [1, 2, 3]}, codec_name=codec_name
    )
    task_id, idx, channel, value = envelope.unpack_write(data)
    assert task_id == "task-1"
    assert idx == 0
    assert channel == "my_channel"
    assert value == {"nested": [1, 2, 3]}
```

- [ ] **Step 4: Run the new tests to verify they fail**

Run: `uv run pytest tests/unit/test_envelope.py -v`
Expected: FAIL — `envelope.resolve_codec` doesn't exist yet, and
`pack_checkpoint`/`pack_write` don't accept `codec_name`.

- [ ] **Step 5: Rewrite `envelope.py` with the codec registry and wrap/unwrap**

Replace the full contents of `src/langgraph_checkpoint_objectstorage/envelope.py`:

```python
from __future__ import annotations

import lzma
import zlib
from typing import Any, Protocol

import ormsgpack
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

try:
    import zstandard
except ImportError:
    zstandard = None

_serde = JsonPlusSerializer()


class Codec(Protocol):
    def compress(self, data: bytes) -> bytes: ...
    def decompress(self, data: bytes) -> bytes: ...


class _Zlib:
    def compress(self, data: bytes) -> bytes:
        return zlib.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return zlib.decompress(data)


class _Lzma:
    def compress(self, data: bytes) -> bytes:
        return lzma.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return lzma.decompress(data)


class _Zstd:
    def __init__(self) -> None:
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

    def compress(self, data: bytes) -> bytes:
        return self._compressor.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._decompressor.decompress(data)


_CODECS: dict[str, Codec] = {"zlib": _Zlib(), "lzma": _Lzma()}
if zstandard is not None:
    _CODECS["zstd"] = _Zstd()

_VALID_COMPRESSION_NAMES = ("none", "zlib", "lzma", "zstd")


def resolve_codec(compression: str) -> str | None:
    if compression == "none":
        return None
    if compression not in _VALID_COMPRESSION_NAMES:
        raise ValueError(
            f"unknown compression {compression!r}; expected one of "
            f"{_VALID_COMPRESSION_NAMES}"
        )
    if compression not in _CODECS:
        raise ImportError(
            f"compression={compression!r} requires the '{compression}' extra: "
            f"pip install 'langgraph-checkpoint-objectstorage[{compression}]'"
        )
    return compression


def _wrap(data: bytes, codec_name: str | None) -> bytes:
    if codec_name is None:
        return data
    return ormsgpack.packb(
        {"codec": codec_name, "payload": _CODECS[codec_name].compress(data)}
    )


def _unwrap(data: bytes) -> bytes:
    obj = ormsgpack.unpackb(data)
    if isinstance(obj, dict) and "codec" in obj:
        codec_name = obj["codec"]
        if codec_name not in _CODECS:
            raise ValueError(
                f"checkpoint object recorded unknown codec {codec_name!r}; "
                "this build of langgraph-checkpoint-objectstorage doesn't "
                "support it"
            )
        return _CODECS[codec_name].decompress(obj["payload"])
    return data


def pack_checkpoint(
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    parent_checkpoint_id: str | None,
    codec_name: str | None = None,
) -> bytes:
    ck_type, ck_bytes = _serde.dumps_typed(checkpoint)
    md_type, md_bytes = _serde.dumps_typed(metadata)
    inner = ormsgpack.packb(
        {
            "checkpoint": [ck_type, ck_bytes],
            "metadata": [md_type, md_bytes],
            "parent_checkpoint_id": parent_checkpoint_id,
        }
    )
    return _wrap(inner, codec_name)


def unpack_checkpoint(
    data: bytes,
) -> tuple[Checkpoint, CheckpointMetadata, str | None]:
    obj = ormsgpack.unpackb(_unwrap(data))
    checkpoint = _serde.loads_typed(tuple(obj["checkpoint"]))
    metadata = _serde.loads_typed(tuple(obj["metadata"]))
    return checkpoint, metadata, obj["parent_checkpoint_id"]


def pack_write(
    task_id: str,
    idx: int,
    channel: str,
    value: Any,
    codec_name: str | None = None,
) -> bytes:
    v_type, v_bytes = _serde.dumps_typed(value)
    inner = ormsgpack.packb(
        {
            "task_id": task_id,
            "idx": idx,
            "channel": channel,
            "type": v_type,
            "value": v_bytes,
        }
    )
    return _wrap(inner, codec_name)


def unpack_write(data: bytes) -> tuple[str, int, str, Any]:
    obj = ormsgpack.unpackb(_unwrap(data))
    value = _serde.loads_typed((obj["type"], obj["value"]))
    return obj["task_id"], obj["idx"], obj["channel"], value
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_envelope.py -v`
Expected: PASS (all existing + new tests)

- [ ] **Step 7: Lint**

Run: `uv run black --check src tests` (if it reformats, run `uv run black src tests` then re-check)
Run: `uv run pyflakes src tests`
Expected: both clean

- [ ] **Step 8: Commit**

```bash
git add pyproject.toml src/langgraph_checkpoint_objectstorage/envelope.py tests/unit/test_envelope.py
git commit -m "feat: add pluggable codec registry to envelope.py for checkpoint compression"
```

---

### Task 2: Wire `compression` into `ObjectStorageSaver`

**Files:**
- Modify: `src/langgraph_checkpoint_objectstorage/saver.py:74-111` (`__init__`), `:224-238` (`_put`), `:283-307` (`_put_writes`)
- Test: `tests/unit/test_compression.py` (new file)

**Interfaces:**
- Consumes: `envelope.resolve_codec(compression: str) -> str | None`, `envelope.pack_checkpoint(..., codec_name=...)`, `envelope.pack_write(..., codec_name=...)` from Task 1.
- Produces: `ObjectStorageSaver.__init__(fs, root, ttl=None, compression="none")` — new keyword-or-positional param, default `"none"` preserves current behavior exactly. Stores resolved codec as `self._codec_name: str | None` (used internally by `_put`/`_put_writes`; Task 3 reads `saver._codec_name` in tests to assert `from_conn_string` wired it through correctly).

- [ ] **Step 1: Write the failing tests**

Create `tests/unit/test_compression.py`:

```python
import os

import fsspec
import pytest

from langgraph_checkpoint_objectstorage import envelope, keys
from langgraph_checkpoint_objectstorage.saver import ObjectStorageSaver


def make_saver(root, compression="none"):
    fs = fsspec.filesystem("file")
    return ObjectStorageSaver(fs, str(root), compression=compression)


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def test_compression_none_is_default(tmp_path):
    saver = make_saver(tmp_path)
    assert saver._codec_name is None


def test_unknown_compression_name_raises_value_error(tmp_path):
    with pytest.raises(ValueError, match="bogus"):
        make_saver(tmp_path, compression="bogus")


def test_zstd_without_extra_raises_import_error_at_construction(tmp_path, monkeypatch):
    monkeypatch.delitem(envelope._CODECS, "zstd", raising=False)
    with pytest.raises(ImportError, match="zstd"):
        make_saver(tmp_path, compression="zstd")


@pytest.mark.parametrize("compression", ["zlib", "lzma", "zstd"])
async def test_put_get_tuple_roundtrip_with_compression(tmp_path, compression):
    saver = make_saver(tmp_path, compression=compression)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "v" * 1000}
    await saver._put(config, checkpoint, {"source": "input", "step": 0}, {})

    tup = await saver._get_tuple(config)
    assert tup.checkpoint["channel_values"] == {"k": "v" * 1000}
    assert tup.metadata["source"] == "input"


async def test_put_writes_roundtrip_with_compression(tmp_path):
    saver = make_saver(tmp_path, compression="zstd")
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver._put_writes(stored, [("ch", "val")], "task-1")

    writes = await saver._read_pending_writes("t1", "", "ckpt-1")
    assert writes == [("task-1", "ch", "val")]


async def test_compressed_object_is_smaller_for_large_repetitive_payload(tmp_path):
    large_value = "x" * 100_000
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

    none_saver = make_saver(tmp_path / "none", compression="none")
    lzma_saver = make_saver(tmp_path / "lzma", compression="lzma")
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": large_value}
    await none_saver._put(config, checkpoint, {"step": 0}, {})
    await lzma_saver._put(config, checkpoint, {"step": 0}, {})

    none_key = keys.checkpoint_key(str(tmp_path / "none"), "t1", "", "ckpt-1")
    lzma_key = keys.checkpoint_key(str(tmp_path / "lzma"), "t1", "", "ckpt-1")
    assert os.path.getsize(lzma_key) < os.path.getsize(none_key)


async def test_saver_reads_checkpoint_written_under_a_different_codec(tmp_path):
    # A fleet reconfiguring `compression` between deploys must still read
    # what earlier deploys wrote, under whichever codec wrote it.
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver_lzma = make_saver(tmp_path, compression="lzma")
    stored1 = await saver_lzma._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    saver_zlib = make_saver(tmp_path, compression="zlib")
    config2 = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": stored1["configurable"]["checkpoint_id"],
        }
    }
    await saver_zlib._put(config2, _checkpoint("ckpt-2"), {"step": 1}, {})

    tup_old = await saver_zlib._get_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": "ckpt-1",
            }
        }
    )
    assert tup_old.checkpoint["id"] == "ckpt-1"
    tup_new = await saver_zlib._get_tuple(config)
    assert tup_new.checkpoint["id"] == "ckpt-2"


async def test_saver_reads_legacy_uncompressed_checkpoint_once_compression_enabled(
    tmp_path,
):
    legacy_saver = make_saver(tmp_path, compression="none")
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await legacy_saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    saver = make_saver(tmp_path, compression="lzma")
    tup = await saver._get_tuple(config)
    assert tup.checkpoint["id"] == "ckpt-1"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_compression.py -v`
Expected: FAIL — `ObjectStorageSaver.__init__` doesn't accept `compression`
yet, and `saver._codec_name` doesn't exist.

- [ ] **Step 3: Add `compression` to `__init__`**

In `src/langgraph_checkpoint_objectstorage/saver.py`, change the `__init__`
signature (currently `saver.py:74-76`):

```python
    @typechecked
    def __init__(
        self,
        fs: AbstractFileSystem,
        root: str,
        ttl: timedelta | None = None,
        compression: str = "none",
    ) -> None:
```

Add a `compression` entry to the docstring's `Args:` block, right after the
existing `ttl` entry:

```python
            compression: Codec used to compress each checkpoint/write
                object before upload: `"none"` (default -- byte-identical
                to the output before this option existed), `"zlib"` or
                `"lzma"` (stdlib, no extra dependency), or `"zstd"`
                (requires the `compression` extra -- raises `ImportError`
                here at construction time if requested without it
                installed). Reads always decode using the codec recorded
                in the object itself, independent of this saver's own
                configured value, so changing `compression` between
                deploys is safe: old and new objects coexist and both stay
                readable.
```

In the body (currently `saver.py:95-98`), add two lines right after
`self.ttl = ttl`:

```python
        super().__init__()
        self.fs = fs
        self.root = root.rstrip("/")
        self.ttl = ttl
        self.compression = compression
        self._codec_name = envelope.resolve_codec(compression)
        self._is_async_native = isinstance(fs, AsyncFileSystem)
```

- [ ] **Step 4: Thread the codec into `_put` and `_put_writes`**

In `_put` (currently `saver.py:236`), change:

```python
        data = envelope.pack_checkpoint(checkpoint, full_metadata, parent_checkpoint_id)
```

to:

```python
        data = envelope.pack_checkpoint(
            checkpoint, full_metadata, parent_checkpoint_id, codec_name=self._codec_name
        )
```

In `_put_writes` (currently `saver.py:306`), change:

```python
            data = envelope.pack_write(task_id, actual_idx, channel, value)
```

to:

```python
            data = envelope.pack_write(
                task_id, actual_idx, channel, value, codec_name=self._codec_name
            )
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_compression.py -v`
Expected: PASS

- [ ] **Step 6: Run the full unit suite to check for regressions**

Run: `uv run pytest tests/unit -v`
Expected: PASS (existing `test_saver_core.py`, `test_saver_public_api.py`,
`test_ttl.py`, `test_logging.py` all still green — none of them pass
`compression`, so they exercise the new default and must behave exactly as
before)

- [ ] **Step 7: Lint**

Run: `uv run black --check src tests` (or `uv run black src tests` then re-check)
Run: `uv run pyflakes src tests`
Run: `uv run bandit -r -q src`
Run: `uv run vulture src vulture_whitelist.py`
Expected: all clean

- [ ] **Step 8: Commit**

```bash
git add src/langgraph_checkpoint_objectstorage/saver.py tests/unit/test_compression.py
git commit -m "feat: add compression option to ObjectStorageSaver.__init__"
```

---

### Task 3: `compression` in `from_conn_string`

**Files:**
- Modify: `src/langgraph_checkpoint_objectstorage/saver.py:1-25` (imports), `:113-137` (`from_conn_string`)
- Test: `tests/unit/test_saver_public_api.py`

**Interfaces:**
- Consumes: `ObjectStorageSaver.__init__(..., compression=...)` from Task 2.
- Produces: `ObjectStorageSaver.from_conn_string(conn_string, *, ttl=None, compression="none", **storage_options)` — `compression` can be passed as an explicit keyword (mirrors `ttl`) or embedded in `conn_string` as a `?compression=...` query parameter (per the ADR's example); the query parameter wins if both are given, and is always stripped from the URI before it reaches `fsspec.core.url_to_fs` (which does not parse query strings itself and would otherwise leave the literal `?compression=...` string stuck onto a local path).

- [ ] **Step 1: Write the failing tests**

Add to `tests/unit/test_saver_public_api.py` (new import at top: add
`from datetime import timedelta` is already implied by `ttl` tests if
present — check the file; here we only need `ObjectStorageSaver`, already
imported):

```python
def test_from_conn_string_compression_kwarg(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(
        f"file://{tmp_path}", compression="zlib"
    )
    assert saver._codec_name == "zlib"


def test_from_conn_string_compression_query_param(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(f"file://{tmp_path}?compression=lzma")
    assert saver._codec_name == "lzma"
    assert saver.root == str(tmp_path).rstrip("/")


def test_from_conn_string_compression_query_param_overrides_kwarg(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(
        f"file://{tmp_path}?compression=lzma", compression="zlib"
    )
    assert saver._codec_name == "lzma"


def test_from_conn_string_default_compression_is_none(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(f"file://{tmp_path}")
    assert saver._codec_name is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/test_saver_public_api.py -v`
Expected: FAIL — `from_conn_string` doesn't accept/parse `compression` yet,
and (for the query-param cases) `saver.root` would literally contain the
`?compression=...` suffix since `fsspec.core.url_to_fs` doesn't strip query
strings on its own.

- [ ] **Step 3: Add the import and rewrite `from_conn_string`**

At the top of `src/langgraph_checkpoint_objectstorage/saver.py`, add to the
existing import block (after the `os` import, alongside the other stdlib
imports):

```python
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
```

Replace `from_conn_string` (currently `saver.py:113-137`):

```python
    @classmethod
    @typechecked
    def from_conn_string(
        cls,
        conn_string: str,
        *,
        ttl: timedelta | None = None,
        compression: str = "none",
        **storage_options: Any,
    ) -> "ObjectStorageSaver":
        """Build a saver from an fsspec connection string.

        Args:
            conn_string: An fsspec URI, e.g. `"file:///path"`,
                `"s3://bucket/prefix"`, or `"gcs://bucket/prefix"`.
                `compression` can be embedded here as a query parameter,
                e.g. `"file:///path?compression=lzma"` -- it wins over the
                `compression` keyword below if both are given, and is
                always stripped before the URI is resolved to a
                filesystem.
            ttl: Forwarded to `__init__` -- see its docstring.
            compression: Forwarded to `__init__` -- see its docstring.
            **storage_options: Forwarded to the underlying fsspec
                filesystem constructor -- useful for explicit credentials
                or a custom S3-compatible endpoint (MinIO, etc.).

        Returns:
            A new `ObjectStorageSaver` backed by the resolved filesystem.
        """
        parsed = urlsplit(conn_string)
        query = parse_qs(parsed.query)
        compression_values = query.pop("compression", None)
        if compression_values:
            compression = compression_values[-1]
            conn_string = urlunsplit(
                parsed._replace(query=urlencode(query, doseq=True))
            )
        storage_options.setdefault("skip_instance_cache", True)
        fs, path = fsspec.core.url_to_fs(conn_string, **storage_options)
        return cls(fs, path, ttl=ttl, compression=compression)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/test_saver_public_api.py -v`
Expected: PASS

- [ ] **Step 5: Run the full unit suite**

Run: `uv run pytest tests/unit -v`
Expected: PASS

- [ ] **Step 6: Lint**

Run: `uv run black --check src tests` (or format then re-check)
Run: `uv run pyflakes src tests`
Expected: clean

- [ ] **Step 7: Commit**

```bash
git add src/langgraph_checkpoint_objectstorage/saver.py tests/unit/test_saver_public_api.py
git commit -m "feat: accept compression via from_conn_string kwarg or query param"
```

---

### Task 4: Conformance coverage with compression on and off

**Files:**
- Modify: `tests/integration/test_conformance_local.py`
- Modify: `tests/integration/test_conformance_s3.py`
- Modify: `tests/integration/test_conformance_gcs.py`

**Interfaces:**
- Consumes: `ObjectStorageSaver(..., compression=...)` (Task 2) and
  `ObjectStorageSaver.from_conn_string(..., compression=...)` (Task 3).
- Produces: nothing new — this task only extends existing test coverage.

- [ ] **Step 1: Parametrize the local conformance test over every codec**

Replace the contents of `tests/integration/test_conformance_local.py`:

```python
import tempfile

import fsspec
import pytest
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


@pytest.mark.parametrize("compression", ["none", "zlib", "lzma", "zstd"])
async def test_local_conformance(compression):
    @checkpointer_test(name=f"ObjectStorageSaver-local-{compression}")
    async def _local_checkpointer():
        with tempfile.TemporaryDirectory() as tmp:
            yield ObjectStorageSaver(
                fsspec.filesystem("file"), tmp, compression=compression
            )

    report = await validate(_local_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/integration/test_conformance_local.py -v`
Expected: PASS, 4 parametrized cases (`none`, `zlib`, `lzma`, `zstd`)

- [ ] **Step 3: Parametrize the S3 conformance test over a representative subset**

Replace the contents of `tests/integration/test_conformance_s3.py`:

```python
import uuid

import pytest
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


@pytest.mark.parametrize("compression", ["none", "zstd"])
async def test_s3_conformance(moto_s3_endpoint, s3_bucket, compression):
    prefix = f"{s3_bucket}/{uuid.uuid4()}"

    @checkpointer_test(name=f"ObjectStorageSaver-s3-{compression}")
    async def _s3_checkpointer():
        yield ObjectStorageSaver.from_conn_string(
            f"s3://{prefix}",
            key="testing",
            secret="testing",
            client_kwargs={"endpoint_url": moto_s3_endpoint},
            compression=compression,
        )

    report = await validate(_s3_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()
```

- [ ] **Step 4: Run it**

Run: `uv run pytest tests/integration/test_conformance_s3.py -v`
Expected: PASS, 2 parametrized cases (`none`, `zstd`)

- [ ] **Step 5: Parametrize the gated real-GCS conformance test the same way**

In `tests/integration/test_conformance_gcs.py`, change the
`test_gcs_conformance` function (leave `test_gcs_protocol_resolves_to_gcsfs`
untouched):

```python
@pytest.mark.skipif(
    not os.environ.get("GCS_TEST_BUCKET"),
    reason="set GCS_TEST_BUCKET (+ GCP credentials) to run real-bucket GCS conformance",
)
@pytest.mark.parametrize("compression", ["none", "zstd"])
async def test_gcs_conformance(compression):
    bucket = os.environ["GCS_TEST_BUCKET"]

    @checkpointer_test(name=f"ObjectStorageSaver-gcs-{compression}")
    async def _gcs_checkpointer():
        prefix = f"{bucket}/{uuid.uuid4()}"
        yield ObjectStorageSaver.from_conn_string(f"gcs://{prefix}", compression=compression)

    report = await validate(_gcs_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()
```

- [ ] **Step 6: Run the full integration suite (skips gated/emulator tests without `make compose-up`, that's expected)**

Run: `uv run pytest tests/integration -v`
Expected: local + moto-backed S3 conformance cases PASS; GCS
real-bucket case SKIPPED (no `GCS_TEST_BUCKET` set) unless the user has it
configured

- [ ] **Step 7: Lint**

Run: `uv run pyflakes src tests`
Expected: clean

- [ ] **Step 8: Commit**

```bash
git add tests/integration/test_conformance_local.py tests/integration/test_conformance_s3.py tests/integration/test_conformance_gcs.py
git commit -m "test: parametrize conformance suites over compression codecs"
```

---

### Task 5: Documentation and ADR status

**Files:**
- Modify: `README.md`
- Modify: `docs/adr/0003-checkpoint-compression.md`

**Interfaces:** none — documentation only.

- [ ] **Step 1: Add a Compression entry to the README table of contents**

In `README.md`, in the "Table of contents" list, add a line right after
`- [Checkpoint TTL](#-checkpoint-ttl)`:

```markdown
- [Compression](#-compression)
```

- [ ] **Step 2: Mention the new extra in Install**

In `README.md`'s `## Install` section, add a third line to the `pip
install` code block:

```bash
pip install langgraph-checkpoint-objectstorage        # local filesystem only
pip install "langgraph-checkpoint-objectstorage[s3]"   # + AWS S3
pip install "langgraph-checkpoint-objectstorage[gcs]"  # + Google Cloud Storage
pip install "langgraph-checkpoint-objectstorage[compression]"  # + zstd codec
```

- [ ] **Step 3: Add a Compression section, mirroring Checkpoint TTL's style**

In `README.md`, insert a new section right after the `## ⏳ Checkpoint TTL`
section ends (right before `## Architecture`, currently line 249):

```markdown
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
before this option existed — nothing changes unless you opt in. Accepted
values:

- `"none"` — no compression (default).
- `"zlib"` / `"lzma"` — standard library, no extra dependency. `lzma`
  compresses smaller but slower than `zlib`.
- `"zstd"` — faster than both at a comparable or better ratio, but needs
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
Compression runs synchronously on the event loop — see
[ADR 0003](docs/adr/0003-checkpoint-compression.md) for the tradeoff.
Compressed objects also lose the plain-`cat`/`aws s3 cp` inspectability
uncompressed objects have.
```

- [ ] **Step 4: Add the ADR to the Architecture decision records list**

In `README.md`'s `## Architecture decision records` section, add a bullet
after the `0001-checkpoint-ttl.md` entry (matching its no-status-suffix
style, since this one is now implemented too):

```markdown
- [`0003-checkpoint-compression.md`](docs/adr/0003-checkpoint-compression.md)
  on the pluggable codec registry and wire format behind the
  `compression` option.
```

- [ ] **Step 5: Flip the ADR's status**

In `docs/adr/0003-checkpoint-compression.md`, change:

```markdown
- **Status:** Proposed
```

to:

```markdown
- **Status:** Accepted (implemented)
```

- [ ] **Step 6: Keep CLAUDE.md's packaging-conventions note in sync**

In `CLAUDE.md`'s "Dependency and packaging conventions" section, change:

```markdown
- New backend extras follow the existing `[project.optional-dependencies]`
  pattern (`s3`, `gcs`) — core `dependencies` stays backend-agnostic
  (`fsspec`, not `s3fs`/`gcsfs`).
```

to:

```markdown
- New backend/feature extras follow the existing
  `[project.optional-dependencies]` pattern (`s3`, `gcs`, `compression`)
  — core `dependencies` stays backend-agnostic (`fsspec`, not
  `s3fs`/`gcsfs`/`zstandard`).
```

- [ ] **Step 7: Run the full test suite one more time end to end**

Run: `make test`
Expected: lint clean, all unit tests pass, integration tests pass (moto S3
runs in-process; GCS/compose-backed cases skip without `make compose-up` —
expected)

- [ ] **Step 8: Commit**

```bash
git add README.md CLAUDE.md docs/adr/0003-checkpoint-compression.md
git commit -m "docs: document the compression option and mark ADR 0003 implemented"
```

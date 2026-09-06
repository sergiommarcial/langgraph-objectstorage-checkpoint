from __future__ import annotations

import asyncio
import inspect
import logging
import os
import threading
import time
import weakref
from collections.abc import (
    AsyncIterator,
    Awaitable,
    Callable,
    Iterator,
    Mapping,
    Sequence,
)
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

import fsspec
from fsspec import AbstractFileSystem
from fsspec.asyn import AsyncFileSystem, sync as fsspec_sync
from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    BaseCheckpointSaver,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    ChannelVersions,
    WRITES_IDX_MAP,
    get_checkpoint_metadata,
)
from typeguard import typechecked

from langgraph_checkpoint_objectstorage import archive, envelope, keys
from langgraph_checkpoint_objectstorage.observability import IOEvent

logger = logging.getLogger("langgraph_checkpoint_objectstorage")

_LOG_LEVEL_ENV = "LANGGRAPH_CHECKPOINT_OBJECTSTORAGE_LOG_LEVEL"


def _check_safe_segment(label: str, value: str, *, allow_empty: bool = False) -> None:
    if value == "" and allow_empty:
        return
    if not keys.is_safe_segment(value):
        raise ValueError(
            f"{label}={value!r} is not a valid path segment: this saver's key "
            f"layout uses it as one segment of a '/'-joined path, so it must be "
            f"non-empty, must not be '.' or '..', and may only contain letters, "
            f"digits, and '.', ':', '-', '_'"
        )


def _thread_ns(config: RunnableConfig) -> tuple[str, str]:
    configurable = config["configurable"]
    thread_id = configurable["thread_id"]
    checkpoint_ns = configurable.get("checkpoint_ns", "")
    _check_safe_segment("thread_id", thread_id)
    _check_safe_segment("checkpoint_ns", checkpoint_ns, allow_empty=True)
    return thread_id, checkpoint_ns


def _cfg(
    thread_id: str, checkpoint_ns: str, checkpoint_id: str | None
) -> RunnableConfig:
    return {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": checkpoint_ns,
            "checkpoint_id": checkpoint_id,
        }
    }


def _mtime_of(info: Mapping[str, Any]) -> datetime:
    raw = info.get("mtime", info.get("LastModified"))
    if raw is None:
        raise ValueError(f"filesystem info has no mtime/LastModified: {info!r}")
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(raw, tz=timezone.utc)
    return raw if raw.tzinfo is not None else raw.replace(tzinfo=timezone.utc)


def _source_thread_id(entries: dict[str, bytes]) -> str:
    thread_ids = {keys.thread_id_from_relative_key(path) for path in entries}
    if len(thread_ids) != 1:
        raise ValueError(
            f"archive contains {len(thread_ids)} distinct thread_id prefixes "
            f"{sorted(thread_ids)!r}; expected exactly one"
        )
    return next(iter(thread_ids))


class _IOTiming:
    """Mutable slot an `_timed_io` body fills in as it learns count/nbytes."""

    __slots__ = ("count", "nbytes", "error")

    def __init__(self) -> None:
        self.count: int | None = None
        self.nbytes: int | None = None
        self.error: BaseException | None = None


_BG_LOOP_JOIN_TIMEOUT = 1.0


def _stop_background_loop(
    loop: asyncio.AbstractEventLoop, thread: threading.Thread
) -> None:
    # Module-level, not a method: weakref.finalize's callback must not
    # reference `self`, or the saver being finalized would never become
    # unreachable in the first place.
    try:
        loop.call_soon_threadsafe(loop.stop)
        thread.join(timeout=_BG_LOOP_JOIN_TIMEOUT)
    except Exception:
        # Interpreter shutdown can tear down thread/module state in an
        # unpredictable order. The loop's daemon thread is the real
        # safety net for process exit; this is best-effort tidiness, so
        # log rather than raise out of a finalizer.
        logger.debug("background loop cleanup failed during finalize", exc_info=True)


class ObjectStorageSaver(BaseCheckpointSaver):
    """LangGraph checkpoint saver backed by local filesystem, GCS, or S3.

    One class for all three backends -- which one is used is decided by
    the fsspec filesystem passed in (or resolved from a URI via
    `from_conn_string`), never by subclassing. Each checkpoint and each
    pending write is stored as its own object under `root`, keyed by
    thread_id/checkpoint_ns so unrelated threads never collide and writes
    never require a read-modify-write on an existing key.

    Example:
        >>> saver = ObjectStorageSaver.from_conn_string("file:///tmp/checkpoints")
        >>> graph = builder.compile(checkpointer=saver)
    """

    @typechecked
    def __init__(
        self,
        fs: AbstractFileSystem,
        root: str,
        ttl: timedelta | None = None,
        compression: str = "none",
        encryption: envelope.KeyProvider | None = None,
        on_io: Callable[[IOEvent], None | Awaitable[None]] | None = None,
    ) -> None:
        """Wrap an existing fsspec filesystem as a checkpoint store.

        Args:
            fs: Any fsspec `AbstractFileSystem` instance (`LocalFileSystem`,
                `S3FileSystem`, `GCSFileSystem`, ...). Async-native
                filesystems (`s3fs`, `gcsfs`) get true async I/O; others
                run through a thread pool.
            root: Root prefix under which every checkpoint and write is
                stored -- a directory path for local filesystems, or a
                "bucket/prefix" path for object storage.
            ttl: Maximum age before a checkpoint or write becomes eligible
                for deletion by `delete_expired`/`adelete_expired`. `None`
                (default) disables TTL. For S3/GCS, this value isn't
                enforced by the saver itself -- configure a bucket
                lifecycle rule filtered by `root` to match (see the
                README's Checkpoint TTL section); `delete_expired` works
                there too as an optional immediate-delete alternative.
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
            encryption: `KeyProvider` implementation used to encrypt each
                checkpoint/write object with AES-256-GCM before upload,
                and decrypt on read. `None` (default) disables encryption
                -- byte-identical to the output before this option
                existed. Requires the `encryption` extra (raises
                `ImportError` here at construction time if requested
                without it installed). Reads always decrypt using the
                `key_id` recorded in the object itself, calling
                `encryption.get_key(thread_id, key_id=...)` to resolve the
                exact historical key, so key rotation and reading objects
                written under a previous `KeyProvider` both work without a
                migration step.
            on_io: Called once per backend I/O call (`find`/`cat`/`pipe`/
                `exists`/`rm`) with an `IOEvent` describing it -- op, key,
                count (for `find`), nbytes (for `cat`/`pipe`), duration,
                and the exception if the call failed (including a
                cancelled call's `asyncio.CancelledError`). `None`
                (default) adds no overhead. May be a plain function or an
                `async def`; an awaitable return value is awaited on the
                same coroutine/thread as the I/O call it observed. Runs
                synchronously in the I/O path, so a slow `on_io` adds real
                latency -- hand off to a queue yourself if you need to
                ship events remotely. On local disk specifically, every
                call shares one persistent background loop/thread per
                saver instance (see ADR 0006), so a slow `on_io` there
                stalls every other concurrent call on that instance, not
                just the one it's timing. An exception raised by `on_io`
                itself is logged at debug level and never propagates, so
                a broken callback can't break real I/O. See
                `otel_on_io_adapter` for a ready-made OpenTelemetry
                adapter.
        """
        super().__init__()
        self.fs = fs
        self.root = root.rstrip("/")
        self.ttl = ttl
        self.compression = compression
        self._codec_name = envelope.resolve_codec(compression)
        self._key_provider = envelope.resolve_key_provider(encryption)
        self._on_io = on_io
        # Strong references for the fire-and-forget `_emit_io` tasks
        # `_timed_io` schedules on cancellation (see there) -- otherwise
        # asyncio can garbage-collect a task with no other referent while
        # it's still pending.
        self._background_io_tasks: set[asyncio.Task[None]] = set()
        self._is_async_native = isinstance(fs, AsyncFileSystem)
        self._bg_loop: asyncio.AbstractEventLoop | None = None
        self._bg_thread: threading.Thread | None = None
        self._bg_loop_lock = threading.Lock()
        level_name = os.environ.get(_LOG_LEVEL_ENV)
        if level_name:
            logger.setLevel(level_name.upper())
        if ttl is not None:
            logger.warning(
                "ttl=%r is set, but ObjectStorageSaver never deletes anything "
                "on its own: configure a bucket lifecycle rule filtered by "
                "root=%r (S3/GCS), or call delete_expired()/adelete_expired() "
                "yourself on a schedule (required for local disk).",
                ttl,
                self.root,
            )

    @classmethod
    @typechecked
    def from_conn_string(
        cls,
        conn_string: str,
        *,
        ttl: timedelta | None = None,
        compression: str = "none",
        encryption: envelope.KeyProvider | None = None,
        on_io: Callable[[IOEvent], None | Awaitable[None]] | None = None,
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
            encryption: Forwarded to `__init__` -- see its docstring.
                Unlike `compression`, this can't be embedded as a
                connection-string query parameter (a `KeyProvider` is an
                object, not a string) -- pass it as a keyword argument
                here.
            on_io: Forwarded to `__init__` -- see its docstring. Like
                `encryption`, this can't be embedded as a connection-string
                query parameter -- pass it as a keyword argument here.
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
        return cls(
            fs,
            path,
            ttl=ttl,
            compression=compression,
            encryption=encryption,
            on_io=on_io,
        )

    def _ensure_bg_loop(self) -> asyncio.AbstractEventLoop:
        # Double-checked: the lock only matters for the first call from
        # possibly several racing threads. Every call after that must
        # stay lock-free, since this runs on every sync call against a
        # non-async-native filesystem.
        if self._bg_loop is not None:
            return self._bg_loop
        with self._bg_loop_lock:
            if self._bg_loop is not None:
                return self._bg_loop
            loop = asyncio.new_event_loop()
            thread = threading.Thread(target=loop.run_forever, daemon=True)
            thread.start()
            weakref.finalize(self, _stop_background_loop, loop, thread)
            self._bg_loop = loop
            self._bg_thread = thread
            return loop

    def _run_sync(self, func, *args, **kwargs):
        if self._is_async_native:
            return fsspec_sync(self.fs.loop, func, *args, **kwargs)
        loop = self._ensure_bg_loop()
        return asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop).result()

    async def _envelope_call(self, func, *args, **kwargs):
        if self._key_provider is None:
            return func(*args, **kwargs)
        # KeyProvider.get_key is real blocking I/O (a KMS/Vault call), so the
        # whole pack/unpack call -- serialize, compress, encrypt/decrypt --
        # runs off the event loop together rather than hopping threads twice
        # for one call. Bundling the (already-fast) serialize/compress work
        # into the same hop is a deliberate simplification: compression-only
        # savers still run inline, since nothing they do blocks.
        return await asyncio.to_thread(
            func, *args, key_provider=self._key_provider, **kwargs
        )

    @asynccontextmanager
    async def _timed_io(
        self, op: Literal["find", "cat", "pipe", "exists", "rm"], key: str
    ):
        # Skipped entirely -- no `monotonic()`, no wrapping -- when no
        # callback is configured, so the documented "adds no overhead"
        # default holds literally, not just approximately.
        if self._on_io is None:
            yield _IOTiming()
            return
        start = time.monotonic()
        timing = _IOTiming()
        try:
            yield timing
        except BaseException as exc:
            # BaseException, not Exception: a cancelled task's
            # asyncio.CancelledError must still be reported to `on_io`
            # rather than looking like a clean success. Always re-raised
            # below, so this never changes what the caller sees.
            timing.error = exc
            raise
        finally:
            emit = self._emit_io(
                op,
                key,
                count=timing.count,
                nbytes=timing.nbytes,
                duration_ms=(time.monotonic() - start) * 1000,
                error=timing.error,
            )
            if isinstance(timing.error, asyncio.CancelledError):
                # A cancelled caller (e.g. a timed-out run) needs to
                # unwind now, not whenever `on_io` gets around to
                # finishing -- fire-and-forget instead of awaiting, so a
                # slow callback can't turn "cancel this" into "cancel
                # this, eventually."
                task = asyncio.get_running_loop().create_task(emit)
                self._background_io_tasks.add(task)
                task.add_done_callback(self._background_io_tasks.discard)
            else:
                await emit

    async def _emit_io(
        self,
        op: Literal["find", "cat", "pipe", "exists", "rm"],
        key: str,
        *,
        count: int | None,
        nbytes: int | None,
        duration_ms: float,
        error: BaseException | None,
    ) -> None:
        event = IOEvent(
            op=op,
            key=key,
            count=count,
            nbytes=nbytes,
            duration_ms=duration_ms,
            error=error,
        )
        try:
            result = self._on_io(event)
            if inspect.isawaitable(result):
                await result
        except Exception:
            logger.debug("on_io callback raised", exc_info=True)

    async def _cat(self, key: str) -> bytes:
        async with self._timed_io("cat", key) as io:
            try:
                if self._is_async_native:
                    data = await self.fs._cat_file(key)
                else:
                    data = await asyncio.to_thread(self.fs.cat_file, key)
            except FileNotFoundError:
                logger.debug("cat key=%s -> not found", key)
                raise
            io.nbytes = len(data)
            logger.debug("cat key=%s -> %d bytes", key, io.nbytes)
            return data

    async def _pipe(self, key: str, data: bytes) -> None:
        async with self._timed_io("pipe", key) as io:
            parent = key.rsplit("/", 1)[0]
            if self._is_async_native:
                await self.fs._makedirs(parent, exist_ok=True)
                await self.fs._pipe_file(key, data)
            else:
                await asyncio.to_thread(self.fs.makedirs, parent, exist_ok=True)
                await asyncio.to_thread(self.fs.pipe_file, key, data)
            io.nbytes = len(data)
            logger.debug("pipe key=%s <- %d bytes", key, io.nbytes)

    async def _find(self, prefix: str) -> list[str]:
        async with self._timed_io("find", prefix) as io:
            try:
                if self._is_async_native:
                    found = await self.fs._find(prefix)
                else:
                    found = await asyncio.to_thread(self.fs.find, prefix)
            except FileNotFoundError:
                logger.debug("find prefix=%s -> not found", prefix)
                raise
            io.count = len(found)
            logger.debug("find prefix=%s -> %d keys", prefix, io.count)
            return found

    async def _find_detailed(self, prefix: str) -> dict[str, dict[str, Any]]:
        # Reported as op="find": it's the same find call with detail=True,
        # not a distinct operation worth its own IOEvent.op value.
        async with self._timed_io("find", prefix) as io:
            try:
                if self._is_async_native:
                    found = await self.fs._find(prefix, detail=True)
                else:
                    found = await asyncio.to_thread(self.fs.find, prefix, detail=True)
            except FileNotFoundError:
                logger.debug("find_detailed prefix=%s -> not found", prefix)
                raise
            io.count = len(found)
            logger.debug("find_detailed prefix=%s -> %d keys", prefix, io.count)
            return found

    async def _exists(self, key: str) -> bool:
        async with self._timed_io("exists", key):
            if self._is_async_native:
                result = await self.fs._exists(key)
            else:
                result = await asyncio.to_thread(self.fs.exists, key)
            logger.debug("exists key=%s -> %s", key, result)
            return result

    async def _rm(self, prefix: str) -> None:
        async with self._timed_io("rm", prefix):
            try:
                if self._is_async_native:
                    await self.fs._rm(prefix, recursive=True)
                else:
                    await asyncio.to_thread(self.fs.rm, prefix, recursive=True)
            except FileNotFoundError:
                logger.debug("rm prefix=%s -> not found", prefix)
                raise
            logger.debug("rm prefix=%s -> removed", prefix)

    async def _read_pending_writes(
        self, thread_id: str, checkpoint_ns: str, checkpoint_id: str
    ) -> list[tuple[str, str, Any]]:
        prefix = keys.writes_prefix(self.root, thread_id, checkpoint_ns, checkpoint_id)
        try:
            write_keys = await self._find(prefix)
        except FileNotFoundError:
            return []
        entries = []
        for key in write_keys:
            data = await self._cat(key)
            path_task_id, path_idx = keys.write_task_id_and_idx_from_key(key)
            entries.append(
                await self._envelope_call(
                    envelope.unpack_write,
                    data,
                    thread_id=thread_id,
                    checkpoint_ns=checkpoint_ns,
                    checkpoint_id=checkpoint_id,
                    task_id=path_task_id,
                    idx=path_idx,
                )
            )
        entries.sort(key=lambda e: (e[0], e[1]))
        return [(task_id, channel, value) for task_id, idx, channel, value in entries]

    async def _put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        thread_id, checkpoint_ns = _thread_ns(config)
        checkpoint_id = checkpoint["id"]
        _check_safe_segment("checkpoint_id", checkpoint_id)
        parent_checkpoint_id = config["configurable"].get("checkpoint_id")
        full_metadata = get_checkpoint_metadata(config, metadata)
        key = keys.checkpoint_key(self.root, thread_id, checkpoint_ns, checkpoint_id)
        data = await self._envelope_call(
            envelope.pack_checkpoint,
            checkpoint,
            full_metadata,
            parent_checkpoint_id,
            codec_name=self._codec_name,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
        )
        await self._pipe(key, data)
        return _cfg(thread_id, checkpoint_ns, checkpoint_id)

    async def _get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        thread_id, checkpoint_ns = _thread_ns(config)
        checkpoint_id = config["configurable"].get("checkpoint_id")
        if checkpoint_id is None:
            logger.debug(
                "get_tuple thread=%s ns=%s -> no checkpoint_id, resolving latest",
                thread_id,
                checkpoint_ns,
            )
            prefix = keys.checkpoints_prefix(self.root, thread_id, checkpoint_ns)
            try:
                candidates = await self._find(prefix)
            except FileNotFoundError:
                return None
            if not candidates:
                return None
            key = max(candidates)
            checkpoint_id = keys.checkpoint_id_from_key(key)
        else:
            _check_safe_segment("checkpoint_id", checkpoint_id)
            key = keys.checkpoint_key(
                self.root, thread_id, checkpoint_ns, checkpoint_id
            )
        try:
            data = await self._cat(key)
        except FileNotFoundError:
            return None
        checkpoint, metadata, parent_checkpoint_id = await self._envelope_call(
            envelope.unpack_checkpoint,
            data,
            thread_id=thread_id,
            checkpoint_ns=checkpoint_ns,
            checkpoint_id=checkpoint_id,
        )
        parent_config = (
            _cfg(thread_id, checkpoint_ns, parent_checkpoint_id)
            if parent_checkpoint_id
            else None
        )
        pending_writes = await self._read_pending_writes(
            thread_id, checkpoint_ns, checkpoint_id
        )
        return CheckpointTuple(
            config=_cfg(thread_id, checkpoint_ns, checkpoint_id),
            checkpoint=checkpoint,
            metadata=metadata,
            parent_config=parent_config,
            pending_writes=pending_writes,
        )

    async def _put_writes(
        self,
        config: RunnableConfig,
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        thread_id, checkpoint_ns = _thread_ns(config)
        checkpoint_id = config["configurable"]["checkpoint_id"]
        _check_safe_segment("checkpoint_id", checkpoint_id)
        _check_safe_segment("task_id", task_id)
        overwrite = all(channel in WRITES_IDX_MAP for channel, _ in writes)
        existing: set[str] = set()
        if not overwrite:
            try:
                existing = set(
                    await self._find(
                        keys.writes_prefix(
                            self.root, thread_id, checkpoint_ns, checkpoint_id
                        )
                    )
                )
            except FileNotFoundError:
                existing = set()
        for idx, (channel, value) in enumerate(writes):
            actual_idx = WRITES_IDX_MAP.get(channel, idx)
            key = keys.write_key(
                self.root, thread_id, checkpoint_ns, checkpoint_id, task_id, actual_idx
            )
            if not overwrite and key in existing:
                logger.debug(
                    "put_writes task=%s channel=%s idx=%s -> skipped, write already exists",
                    task_id,
                    channel,
                    actual_idx,
                )
                continue
            data = await self._envelope_call(
                envelope.pack_write,
                task_id,
                actual_idx,
                channel,
                value,
                codec_name=self._codec_name,
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                checkpoint_id=checkpoint_id,
            )
            await self._pipe(key, data)

    async def _list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ):
        thread_id, checkpoint_ns = _thread_ns(config)
        prefix = keys.checkpoints_prefix(self.root, thread_id, checkpoint_ns)
        try:
            candidate_keys = sorted(await self._find(prefix), reverse=True)
        except FileNotFoundError:
            return
        before_id = before["configurable"]["checkpoint_id"] if before else None
        count = 0
        for key in candidate_keys:
            checkpoint_id = keys.checkpoint_id_from_key(key)
            if before_id is not None and checkpoint_id >= before_id:
                continue
            data = await self._cat(key)
            checkpoint, metadata, parent_checkpoint_id = await self._envelope_call(
                envelope.unpack_checkpoint,
                data,
                thread_id=thread_id,
                checkpoint_ns=checkpoint_ns,
                checkpoint_id=checkpoint_id,
            )
            if filter and not all(metadata.get(k) == v for k, v in filter.items()):
                continue
            parent_config = (
                _cfg(thread_id, checkpoint_ns, parent_checkpoint_id)
                if parent_checkpoint_id
                else None
            )
            pending_writes = await self._read_pending_writes(
                thread_id, checkpoint_ns, checkpoint_id
            )
            yield CheckpointTuple(
                config=_cfg(thread_id, checkpoint_ns, checkpoint_id),
                checkpoint=checkpoint,
                metadata=metadata,
                parent_config=parent_config,
                pending_writes=pending_writes,
            )
            count += 1
            if limit is not None and count >= limit:
                return

    async def _delete_thread(self, thread_id: str) -> None:
        _check_safe_segment("thread_id", thread_id)
        prefix = keys.thread_prefix(self.root, thread_id)
        try:
            await self._rm(prefix)
        except FileNotFoundError:
            pass

    async def _export_thread(self, thread_id: str) -> bytes:
        _check_safe_segment("thread_id", thread_id)
        prefix = keys.thread_prefix(self.root, thread_id)
        try:
            found = await self._find(prefix)
        except FileNotFoundError:
            found = []
        if not found:
            raise KeyError(thread_id)
        entries = {}
        for key in found:
            data = await self._cat(key)
            entries[keys.relative_key(self.root, key)] = data
        return archive.pack(entries)

    async def _import_thread(
        self,
        archive_bytes: bytes,
        *,
        dest_thread_id: str | None = None,
        overwrite: bool = False,
    ) -> None:
        entries = archive.unpack(archive_bytes)
        source_thread_id = _source_thread_id(entries)
        if dest_thread_id is not None:
            _check_safe_segment("dest_thread_id", dest_thread_id)
        target_thread_id = (
            dest_thread_id if dest_thread_id is not None else source_thread_id
        )
        dest_entries = {
            keys.rekeyed_path(self.root, rel, target_thread_id): data
            for rel, data in entries.items()
        }
        if not overwrite:
            try:
                existing = set(
                    await self._find(keys.thread_prefix(self.root, target_thread_id))
                )
            except FileNotFoundError:
                existing = set()
            conflicts = existing & dest_entries.keys()
            if conflicts:
                raise FileExistsError(sorted(conflicts)[0])
        for key, data in dest_entries.items():
            await self._pipe(key, data)

    async def _delete_expired(self) -> None:
        if self.ttl is None:
            return
        cutoff = datetime.now(timezone.utc) - self.ttl
        try:
            detail = await self._find_detailed(self.root)
        except FileNotFoundError:
            return
        for key, info in detail.items():
            if _mtime_of(info) < cutoff:
                await self._rm(key)

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Async variant of `get_tuple`. See `get_tuple` for details."""
        return await self._get_tuple(config)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Async variant of `list`. See `list` for details."""
        async for tup in self._list(config, filter=filter, before=before, limit=limit):
            yield tup

    @typechecked
    async def aput(
        self,
        config: Mapping[str, Any],
        checkpoint: Mapping[str, Any],
        metadata: Mapping[str, Any],
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Async variant of `put`. See `put` for details."""
        return await self._put(config, checkpoint, metadata, new_versions)

    @typechecked
    async def aput_writes(
        self,
        config: Mapping[str, Any],
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Async variant of `put_writes`. See `put_writes` for details."""
        await self._put_writes(config, writes, task_id, task_path)

    @typechecked
    async def adelete_thread(self, thread_id: str) -> None:
        """Async variant of `delete_thread`. See `delete_thread` for details."""
        await self._delete_thread(thread_id)

    @typechecked
    async def aexport_thread(self, thread_id: str) -> bytes:
        """Async variant of `export_thread`. See `export_thread` for details."""
        return await self._export_thread(thread_id)

    @typechecked
    async def aimport_thread(
        self,
        archive_bytes: bytes,
        *,
        dest_thread_id: str | None = None,
        overwrite: bool = False,
    ) -> None:
        """Async variant of `import_thread`. See `import_thread` for details."""
        await self._import_thread(
            archive_bytes, dest_thread_id=dest_thread_id, overwrite=overwrite
        )

    async def adelete_expired(self) -> None:
        """Async variant of `delete_expired`. See `delete_expired` for details."""
        await self._delete_expired()

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Fetch a checkpoint tuple for the given configuration.

        If `config["configurable"]` has no `"checkpoint_id"`, returns the
        latest checkpoint in that thread/namespace.

        Args:
            config: Must contain `configurable.thread_id`. Optionally
                `configurable.checkpoint_ns` (default `""`) and
                `configurable.checkpoint_id` for an exact checkpoint
                rather than the latest.

        Returns:
            The matching `CheckpointTuple`, or `None` if no checkpoint
            exists for that thread/namespace/id -- never raises for
            "not found".

        Raises:
            ValueError: If `thread_id` or an explicit `checkpoint_id` is
                empty, `.`, `..`, or contains `/` or `\\`, or if
                `checkpoint_ns` is `.`, `..`, or contains `/` or `\\`
                (`checkpoint_ns=""`, the
                default namespace, is always valid). This saver's key
                layout uses each as one segment of a `/`-joined path, so a
                value like that could collide with or escape a different
                thread_id/checkpoint_ns/checkpoint_id's storage keys.
        """
        return self._run_sync(self._get_tuple, config)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """List checkpoints for a thread/namespace, newest first.

        Args:
            config: Must contain `configurable.thread_id` and, optionally,
                `configurable.checkpoint_ns` (default `""`).
            filter: Metadata key/value pairs a checkpoint must match
                (applied client-side -- see the README's Known
                limitations section).
            before: Only return checkpoints older than
                `before["configurable"]["checkpoint_id"]`.
            limit: Maximum number of checkpoints to return.

        Returns:
            An iterator of matching `CheckpointTuple`s, newest first. This
            sync version collects all results eagerly before yielding the
            first one (it wraps the async implementation via
            `asyncio.run`, which can't stream lazily) -- use `alist` from
            async code for true streaming.

        Raises:
            ValueError: If `thread_id` is empty, `.`, `..`, or contains
                `/` or `\\`, or if `checkpoint_ns` is `.`, `..`, or
                contains `/` or `\\` (`checkpoint_ns=""`, the default
                namespace, is always valid). This saver's key layout uses
                each as one segment of a `/`-joined path, so a value like
                that could collide with or escape a different
                thread_id/checkpoint_ns pair's storage keys.
        """

        async def _collect() -> list[CheckpointTuple]:
            return [
                t
                async for t in self._list(
                    config, filter=filter, before=before, limit=limit
                )
            ]

        yield from self._run_sync(_collect)

    @typechecked
    def put(
        self,
        config: Mapping[str, Any],
        checkpoint: Mapping[str, Any],
        metadata: Mapping[str, Any],
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Store a checkpoint as its own object.

        Args:
            config: Must contain `configurable.thread_id`. If
                `configurable.checkpoint_id` is set, the new checkpoint's
                parent is set to that id; otherwise it has no parent.
            checkpoint: The checkpoint to store, as produced by LangGraph.
                Stored opaquely (see the README's Runtime type checking
                section) -- never hand-extracted field by field.
            metadata: Metadata to store alongside the checkpoint.
            new_versions: Unused by this saver -- accepted for
                `BaseCheckpointSaver` contract compatibility.

        Returns:
            The config to use to fetch this exact checkpoint later
            (`configurable.thread_id`/`checkpoint_ns`/`checkpoint_id`).

        Raises:
            ValueError: If `thread_id` or `checkpoint["id"]` is empty,
                `.`, `..`, or contains `/` or `\\`, or if `checkpoint_ns`
                is `.`, `..`, or contains `/` or `\\` (`checkpoint_ns=""`,
                the default
                namespace, is always valid). This saver's key layout uses
                each as one segment of a `/`-joined path, so a value like
                that could collide with or escape a different
                thread_id/checkpoint_ns/checkpoint_id's storage keys.
        """
        return self._run_sync(self._put, config, checkpoint, metadata, new_versions)

    @typechecked
    def put_writes(
        self,
        config: Mapping[str, Any],
        writes: Sequence[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Store pending writes linked to a checkpoint.

        Regular channels use "first write wins" -- a duplicate
        `(task_id, idx)` is silently ignored, so a retried task can't
        clobber a write another task already committed. The control
        channels (`ERROR`, `SCHEDULED`, `INTERRUPT`, `RESUME`) always
        overwrite instead, since they must reflect the latest state.

        Args:
            config: Must contain `configurable.thread_id` and
                `configurable.checkpoint_id` -- the checkpoint these
                writes are pending against.
            writes: `(channel, value)` pairs to store.
            task_id: Identifier for the task that produced these writes.
            task_path: Unused by this saver -- accepted for
                `BaseCheckpointSaver` contract compatibility.

        Raises:
            ValueError: If `thread_id`, `checkpoint_id`, or `task_id` is
                empty, `.`, `..`, or contains `/` or `\\`, or if
                `checkpoint_ns` is `.`, `..`, or contains `/` or `\\`
                (`checkpoint_ns=""`, the
                default namespace, is always valid). This saver's key
                layout uses each as one segment of a `/`-joined path, so a
                value like that could collide with or escape a different
                thread/checkpoint/task's storage keys.
        """
        self._run_sync(self._put_writes, config, writes, task_id, task_path)

    @typechecked
    def delete_thread(self, thread_id: str) -> None:
        """Delete every checkpoint and write for a thread, across all namespaces.

        A no-op if the thread doesn't exist.

        Args:
            thread_id: The thread to delete.

        Raises:
            ValueError: If `thread_id` is empty, `.`, `..`, or contains
                `/` or `\\` -- this saver's key layout uses it as one
                segment of a `/`-joined path, so a value like that could
                collide with or escape a different thread_id's storage
                keys.
        """
        self._run_sync(self._delete_thread, thread_id)

    @typechecked
    def export_thread(self, thread_id: str) -> bytes:
        """Export a thread's full checkpoint history as an opaque archive.

        Walks every checkpoint and write under `thread_id`, across every
        `checkpoint_ns`, and packs the raw stored bytes into a tar
        archive. The archive is opaque -- whatever compression or
        encryption produced the underlying bytes stays exactly as-is, and
        only `import_thread` can read it back.

        Args:
            thread_id: The thread to export.

        Returns:
            A tar-archive-formatted `bytes` object, suitable for backup or
            for passing to `import_thread`.

        Raises:
            KeyError: If `thread_id` has no checkpoints.
            ValueError: If `thread_id` is empty, `.`, `..`, or contains
                `/` or `\\` -- the archive's flat path layout can't
                represent it unambiguously.
        """
        return self._run_sync(self._export_thread, thread_id)

    @typechecked
    def import_thread(
        self,
        archive_bytes: bytes,
        *,
        dest_thread_id: str | None = None,
        overwrite: bool = False,
    ) -> None:
        """Import an archive produced by `export_thread`.

        Args:
            archive_bytes: Bytes produced by a prior `export_thread` call.
            dest_thread_id: Thread to import into. Defaults to the
                thread_id the archive was exported from. Renaming an
                encrypted thread this way permanently breaks decryption
                (AES-256-GCM's associated data is bound to `thread_id` --
                see the README's Encryption section); only rename if
                you're re-encrypting under the new `thread_id` yourself
                before import.
            overwrite: If `False` (default) and any destination key
                already exists, raises without writing anything. Set
                `True` to replace existing data.

        Raises:
            ValueError: If `archive_bytes` isn't a well-formed
                single-thread archive (including an unsafe or malformed
                entry path), or if `dest_thread_id` is empty, `.`, `..`,
                or contains `/` or `\\`.
            FileExistsError: If a destination key already exists and
                `overwrite` is `False`.
        """
        self._run_sync(
            self._import_thread,
            archive_bytes,
            dest_thread_id=dest_thread_id,
            overwrite=overwrite,
        )

    def delete_expired(self) -> None:
        """Delete every checkpoint and write older than `ttl`.

        A no-op if `ttl` is `None` (the default). Deletion is per-object
        age, not per-checkpoint-chain: a checkpoint and the writes added
        to it later via `put_writes` age out independently, so they can
        expire at slightly different times. See the README's Checkpoint
        TTL section.
        """
        self._run_sync(self._delete_expired)

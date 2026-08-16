from __future__ import annotations

import asyncio
import logging
import os
from collections.abc import AsyncIterator, Iterator, Mapping, Sequence
from typing import Any

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

from langgraph_checkpoint_objectstorage import envelope, keys

logger = logging.getLogger("langgraph_checkpoint_objectstorage")

_LOG_LEVEL_ENV = "LANGGRAPH_CHECKPOINT_OBJECTSTORAGE_LOG_LEVEL"


def _thread_ns(config: RunnableConfig) -> tuple[str, str]:
    configurable = config["configurable"]
    return configurable["thread_id"], configurable.get("checkpoint_ns", "")


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
    def __init__(self, fs: AbstractFileSystem, root: str) -> None:
        """Wrap an existing fsspec filesystem as a checkpoint store.

        Args:
            fs: Any fsspec `AbstractFileSystem` instance (`LocalFileSystem`,
                `S3FileSystem`, `GCSFileSystem`, ...). Async-native
                filesystems (`s3fs`, `gcsfs`) get true async I/O; others
                run through a thread pool.
            root: Root prefix under which every checkpoint and write is
                stored -- a directory path for local filesystems, or a
                "bucket/prefix" path for object storage.
        """
        super().__init__()
        self.fs = fs
        self.root = root.rstrip("/")
        self._is_async_native = isinstance(fs, AsyncFileSystem)
        level_name = os.environ.get(_LOG_LEVEL_ENV)
        if level_name:
            logger.setLevel(level_name.upper())

    @classmethod
    @typechecked
    def from_conn_string(
        cls, conn_string: str, **storage_options: Any
    ) -> "ObjectStorageSaver":
        """Build a saver from an fsspec connection string.

        Args:
            conn_string: An fsspec URI, e.g. `"file:///path"`,
                `"s3://bucket/prefix"`, or `"gcs://bucket/prefix"`.
            **storage_options: Forwarded to the underlying fsspec
                filesystem constructor -- useful for explicit credentials
                or a custom S3-compatible endpoint (MinIO, etc.).

        Returns:
            A new `ObjectStorageSaver` backed by the resolved filesystem.
        """
        storage_options.setdefault("skip_instance_cache", True)
        fs, path = fsspec.core.url_to_fs(conn_string, **storage_options)
        return cls(fs, path)

    def _run_sync(self, func, *args, **kwargs):
        if self._is_async_native:
            return fsspec_sync(self.fs.loop, func, *args, **kwargs)
        return asyncio.run(func(*args, **kwargs))

    async def _cat(self, key: str) -> bytes:
        try:
            if self._is_async_native:
                data = await self.fs._cat_file(key)
            else:
                data = await asyncio.to_thread(self.fs.cat_file, key)
        except FileNotFoundError:
            logger.debug("cat key=%s -> not found", key)
            raise
        logger.debug("cat key=%s -> %d bytes", key, len(data))
        return data

    async def _pipe(self, key: str, data: bytes) -> None:
        parent = key.rsplit("/", 1)[0]
        if self._is_async_native:
            await self.fs._makedirs(parent, exist_ok=True)
            await self.fs._pipe_file(key, data)
        else:
            await asyncio.to_thread(self.fs.makedirs, parent, exist_ok=True)
            await asyncio.to_thread(self.fs.pipe_file, key, data)
        logger.debug("pipe key=%s <- %d bytes", key, len(data))

    async def _find(self, prefix: str) -> list[str]:
        try:
            if self._is_async_native:
                found = await self.fs._find(prefix)
            else:
                found = await asyncio.to_thread(self.fs.find, prefix)
        except FileNotFoundError:
            logger.debug("find prefix=%s -> not found", prefix)
            raise
        logger.debug("find prefix=%s -> %d keys", prefix, len(found))
        return found

    async def _exists(self, key: str) -> bool:
        if self._is_async_native:
            result = await self.fs._exists(key)
        else:
            result = await asyncio.to_thread(self.fs.exists, key)
        logger.debug("exists key=%s -> %s", key, result)
        return result

    async def _rm(self, prefix: str) -> None:
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
            entries.append(envelope.unpack_write(data))
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
        parent_checkpoint_id = config["configurable"].get("checkpoint_id")
        full_metadata = get_checkpoint_metadata(config, metadata)
        key = keys.checkpoint_key(self.root, thread_id, checkpoint_ns, checkpoint_id)
        data = envelope.pack_checkpoint(checkpoint, full_metadata, parent_checkpoint_id)
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
            key = keys.checkpoint_key(
                self.root, thread_id, checkpoint_ns, checkpoint_id
            )
        try:
            data = await self._cat(key)
        except FileNotFoundError:
            return None
        checkpoint, metadata, parent_checkpoint_id = envelope.unpack_checkpoint(data)
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
        overwrite = all(channel in WRITES_IDX_MAP for channel, _ in writes)
        for idx, (channel, value) in enumerate(writes):
            actual_idx = WRITES_IDX_MAP.get(channel, idx)
            key = keys.write_key(
                self.root, thread_id, checkpoint_ns, checkpoint_id, task_id, actual_idx
            )
            if not overwrite and await self._exists(key):
                logger.debug(
                    "put_writes task=%s channel=%s idx=%s -> skipped, write already exists",
                    task_id,
                    channel,
                    actual_idx,
                )
                continue
            data = envelope.pack_write(task_id, actual_idx, channel, value)
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
            checkpoint, metadata, parent_checkpoint_id = envelope.unpack_checkpoint(
                data
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
        prefix = keys.thread_prefix(self.root, thread_id)
        try:
            await self._rm(prefix)
        except FileNotFoundError:
            pass

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
        """
        self._run_sync(self._put_writes, config, writes, task_id, task_path)

    @typechecked
    def delete_thread(self, thread_id: str) -> None:
        """Delete every checkpoint and write for a thread, across all namespaces.

        A no-op if the thread doesn't exist.

        Args:
            thread_id: The thread to delete.
        """
        self._run_sync(self._delete_thread, thread_id)

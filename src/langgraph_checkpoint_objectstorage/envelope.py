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

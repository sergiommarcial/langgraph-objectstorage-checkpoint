from __future__ import annotations

import lzma
import os
import zlib
from typing import Any, Protocol, runtime_checkable

import ormsgpack
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

try:
    import zstandard
except ImportError:
    zstandard = None

try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
except ImportError:
    AESGCM = None

_serde = JsonPlusSerializer()


class Codec(Protocol):
    def compress(self, data: bytes) -> bytes: ...
    def decompress(self, data: bytes) -> bytes: ...


@runtime_checkable
class KeyProvider(Protocol):
    """Supplies AES-256-GCM keys for `ObjectStorageSaver`'s `encryption` option.

    Implement this to wire up a KMS, Vault, or static-key setup. The
    library never talks to a key backend directly -- it only calls
    `get_key`.
    """

    def get_key(self, thread_id: str, key_id: str | None = None) -> tuple[str, bytes]:
        """Return the AES-256 key to use for `thread_id`.

        Args:
            thread_id: The checkpoint thread this key is for.
            key_id: `None` on write, asking for the current key -- return
                any `(key_id, key)` pair identifying it. On read, the
                exact `key_id` recorded in the object being decrypted --
                must resolve to the same key it was encrypted under, even
                after rotating to a new current key.

        Returns:
            A `(key_id, key)` tuple. `key` must be exactly 32 bytes.
        """
        ...


class _Zstd:
    def __init__(self) -> None:
        self._compressor = zstandard.ZstdCompressor()
        self._decompressor = zstandard.ZstdDecompressor()

    def compress(self, data: bytes) -> bytes:
        return self._compressor.compress(data)

    def decompress(self, data: bytes) -> bytes:
        return self._decompressor.decompress(data)


_CODECS: dict[str, Codec] = {"zlib": zlib, "lzma": lzma}
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


_ENC_NAME = "aes-256-gcm"


def _require_aesgcm() -> None:
    if AESGCM is None:
        raise ImportError(
            "encryption requires the 'encryption' extra: "
            "pip install 'langgraph-checkpoint-objectstorage[encryption]'"
        )


def resolve_key_provider(encryption: KeyProvider | None) -> KeyProvider | None:
    if encryption is None:
        return None
    _require_aesgcm()
    if not isinstance(encryption, KeyProvider):
        raise TypeError(
            f"encryption={encryption!r} does not implement the KeyProvider "
            "protocol (missing get_key)"
        )
    return encryption


def _aad(
    thread_id: str | None,
    checkpoint_ns: str | None,
    checkpoint_id: str | None,
    extra: tuple[str | None, ...] = (),
) -> bytes:
    parts = [thread_id, checkpoint_ns, checkpoint_id, *extra]
    if any(part is None for part in parts):
        raise ValueError(
            "encryption requires thread_id, checkpoint_ns, and checkpoint_id "
            "(and task_id/idx for writes) to bind the ciphertext to its "
            "storage location"
        )
    return "\x00".join(parts).encode()


def _wrap_enc(
    data: bytes,
    thread_id: str | None,
    checkpoint_ns: str | None,
    checkpoint_id: str | None,
    key_provider: KeyProvider,
    extra_aad: tuple[str | None, ...] = (),
) -> bytes:
    _require_aesgcm()
    key_id, key = key_provider.get_key(thread_id)
    nonce = os.urandom(12)
    ciphertext = AESGCM(key).encrypt(
        nonce, data, _aad(thread_id, checkpoint_ns, checkpoint_id, extra_aad)
    )
    return ormsgpack.packb(
        {"enc": _ENC_NAME, "key_id": key_id, "nonce": nonce, "payload": ciphertext}
    )


def _decrypt_layer(
    obj: dict,
    thread_id: str | None,
    checkpoint_ns: str | None,
    checkpoint_id: str | None,
    key_provider: KeyProvider | None,
    extra_aad: tuple[str | None, ...] = (),
) -> bytes:
    if obj["enc"] != _ENC_NAME:
        raise ValueError(
            f"checkpoint object recorded unknown encryption {obj['enc']!r}; "
            "this build of langgraph-checkpoint-objectstorage doesn't "
            "support it"
        )
    if key_provider is None:
        raise ValueError(
            f"object is encrypted under key_id={obj['key_id']!r} but no "
            "KeyProvider is configured on this saver"
        )
    _require_aesgcm()
    _, key = key_provider.get_key(thread_id, key_id=obj["key_id"])
    return AESGCM(key).decrypt(
        obj["nonce"],
        obj["payload"],
        _aad(thread_id, checkpoint_ns, checkpoint_id, extra_aad),
    )


def _decompress_layer(obj: dict) -> bytes:
    codec_name = obj["codec"]
    if codec_name not in _CODECS:
        raise ValueError(
            f"checkpoint object recorded unknown codec {codec_name!r}; "
            "this build of langgraph-checkpoint-objectstorage doesn't "
            "support it"
        )
    return _CODECS[codec_name].decompress(obj["payload"])


def _pack_obj(
    obj: dict,
    codec_name: str | None,
    thread_id: str | None = None,
    checkpoint_ns: str | None = None,
    checkpoint_id: str | None = None,
    key_provider: KeyProvider | None = None,
    extra_aad: tuple[str | None, ...] = (),
) -> bytes:
    compressed = _wrap(ormsgpack.packb(obj), codec_name)
    if key_provider is None:
        return compressed
    return _wrap_enc(
        compressed, thread_id, checkpoint_ns, checkpoint_id, key_provider, extra_aad
    )


def _unpack_obj(
    data: bytes,
    thread_id: str | None = None,
    checkpoint_ns: str | None = None,
    checkpoint_id: str | None = None,
    key_provider: KeyProvider | None = None,
    extra_aad: tuple[str | None, ...] = (),
) -> dict:
    # One unpack per layer actually present, not a fixed count: the common
    # case (no encryption, no compression) costs exactly one unpackb, not
    # one per layer this object doesn't have.
    obj = ormsgpack.unpackb(data)
    if isinstance(obj, dict) and "enc" in obj:
        obj = ormsgpack.unpackb(
            _decrypt_layer(
                obj, thread_id, checkpoint_ns, checkpoint_id, key_provider, extra_aad
            )
        )
    elif key_provider is not None:
        raise ValueError(
            "this saver has encryption configured, but the object being "
            "read was never encrypted"
        )

    if isinstance(obj, dict) and "codec" in obj:
        obj = ormsgpack.unpackb(_decompress_layer(obj))

    return obj


def pack_checkpoint(
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    parent_checkpoint_id: str | None,
    codec_name: str | None = None,
    thread_id: str | None = None,
    checkpoint_ns: str | None = None,
    checkpoint_id: str | None = None,
    key_provider: KeyProvider | None = None,
) -> bytes:
    ck_type, ck_bytes = _serde.dumps_typed(checkpoint)
    md_type, md_bytes = _serde.dumps_typed(metadata)
    return _pack_obj(
        {
            "checkpoint": [ck_type, ck_bytes],
            "metadata": [md_type, md_bytes],
            "parent_checkpoint_id": parent_checkpoint_id,
        },
        codec_name,
        thread_id=thread_id,
        checkpoint_ns=checkpoint_ns,
        checkpoint_id=checkpoint_id,
        key_provider=key_provider,
    )


def unpack_checkpoint(
    data: bytes,
    thread_id: str | None = None,
    checkpoint_ns: str | None = None,
    checkpoint_id: str | None = None,
    key_provider: KeyProvider | None = None,
) -> tuple[Checkpoint, CheckpointMetadata, str | None]:
    obj = _unpack_obj(
        data,
        thread_id=thread_id,
        checkpoint_ns=checkpoint_ns,
        checkpoint_id=checkpoint_id,
        key_provider=key_provider,
    )
    checkpoint = _serde.loads_typed(tuple(obj["checkpoint"]))
    metadata = _serde.loads_typed(tuple(obj["metadata"]))
    return checkpoint, metadata, obj["parent_checkpoint_id"]


def pack_write(
    task_id: str,
    idx: int,
    channel: str,
    value: Any,
    codec_name: str | None = None,
    thread_id: str | None = None,
    checkpoint_ns: str | None = None,
    checkpoint_id: str | None = None,
    key_provider: KeyProvider | None = None,
) -> bytes:
    v_type, v_bytes = _serde.dumps_typed(value)
    return _pack_obj(
        {
            "task_id": task_id,
            "idx": idx,
            "channel": channel,
            "type": v_type,
            "value": v_bytes,
        },
        codec_name,
        thread_id=thread_id,
        checkpoint_ns=checkpoint_ns,
        checkpoint_id=checkpoint_id,
        key_provider=key_provider,
        # A write's real identity is finer than its checkpoint: two writes
        # under the same checkpoint still occupy distinct (task_id, idx)
        # paths (keys.write_key), so both must be part of the AAD too --
        # otherwise one write's ciphertext could be relocated onto another
        # write's path within the same checkpoint undetected.
        extra_aad=(task_id, str(idx)),
    )


def unpack_write(
    data: bytes,
    thread_id: str | None = None,
    checkpoint_ns: str | None = None,
    checkpoint_id: str | None = None,
    task_id: str | None = None,
    idx: int | None = None,
    key_provider: KeyProvider | None = None,
) -> tuple[str, int, str, Any]:
    obj = _unpack_obj(
        data,
        thread_id=thread_id,
        checkpoint_ns=checkpoint_ns,
        checkpoint_id=checkpoint_id,
        key_provider=key_provider,
        extra_aad=(task_id, None if idx is None else str(idx)),
    )
    value = _serde.loads_typed((obj["type"], obj["value"]))
    return obj["task_id"], obj["idx"], obj["channel"], value

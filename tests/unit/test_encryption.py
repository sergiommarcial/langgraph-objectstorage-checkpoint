import os
import shutil

import fsspec
import pytest
from cryptography.exceptions import InvalidTag

from langgraph_checkpoint_objectstorage import envelope, keys
from langgraph_checkpoint_objectstorage.saver import ObjectStorageSaver


class _StaticKeyProvider:
    def __init__(self, key_id: str = "k1", key: bytes = b"0" * 32):
        self.key_id = key_id
        self.key = key

    def get_key(self, thread_id, key_id=None):
        return self.key_id, self.key


def make_saver(root, encryption=None, compression="none"):
    fs = fsspec.filesystem("file")
    return ObjectStorageSaver(
        fs, str(root), compression=compression, encryption=encryption
    )


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def test_encryption_none_is_default(tmp_path):
    saver = make_saver(tmp_path)
    assert saver._key_provider is None


def test_key_provider_without_extra_raises_import_error_at_construction(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(envelope, "AESGCM", None)
    with pytest.raises(ImportError, match="encryption"):
        make_saver(tmp_path, encryption=_StaticKeyProvider())


async def test_put_get_tuple_roundtrip_with_encryption(tmp_path):
    saver = make_saver(tmp_path, encryption=_StaticKeyProvider())
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "v" * 1000}
    await saver._put(config, checkpoint, {"source": "input", "step": 0}, {})

    tup = await saver._get_tuple(config)
    assert tup.checkpoint["channel_values"] == {"k": "v" * 1000}
    assert tup.metadata["source"] == "input"


async def test_put_writes_roundtrip_with_encryption(tmp_path):
    saver = make_saver(tmp_path, encryption=_StaticKeyProvider())
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver._put_writes(stored, [("ch", "val")], "task-1")

    writes = await saver._read_pending_writes("t1", "", "ckpt-1")
    assert writes == [("task-1", "ch", "val")]


async def test_put_writes_roundtrip_with_compression_and_encryption(tmp_path):
    saver = make_saver(tmp_path, encryption=_StaticKeyProvider(), compression="zlib")
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "v" * 1000}
    await saver._put(config, checkpoint, {"step": 0}, {})

    tup = await saver._get_tuple(config)
    assert tup.checkpoint["channel_values"] == {"k": "v" * 1000}


async def test_object_bytes_are_not_plaintext_when_encrypted(tmp_path):
    saver = make_saver(tmp_path, encryption=_StaticKeyProvider())
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "a-very-recognizable-secret-value"}
    await saver._put(config, checkpoint, {"step": 0}, {})

    key = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    with open(key, "rb") as f:
        raw = f.read()
    assert b"a-very-recognizable-secret-value" not in raw


async def test_saver_rejects_checkpoint_copied_to_a_different_thread(tmp_path):
    # Simulates an object copied between thread paths on a shared bucket:
    # AAD binding must make the wrong-thread read fail loudly, not
    # silently return misattributed content.
    provider = _StaticKeyProvider()
    saver = make_saver(tmp_path, encryption=provider)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    src = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    dst = keys.checkpoint_key(str(tmp_path), "t2", "", "ckpt-1")
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copyfile(src, dst)

    with pytest.raises(InvalidTag):
        await saver._get_tuple(
            {"configurable": {"thread_id": "t2", "checkpoint_ns": ""}}
        )


async def test_saver_rejects_checkpoint_copied_to_a_different_checkpoint_id(tmp_path):
    # Same idea as the cross-thread case, but within a single thread: a
    # checkpoint object swapped onto a different checkpoint_id must also
    # fail to decrypt, not just a cross-thread move.
    provider = _StaticKeyProvider()
    saver = make_saver(tmp_path, encryption=provider)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    src = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    dst = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-2")
    shutil.copyfile(src, dst)

    with pytest.raises(InvalidTag):
        await saver._get_tuple(
            {
                "configurable": {
                    "thread_id": "t1",
                    "checkpoint_ns": "",
                    "checkpoint_id": "ckpt-2",
                }
            }
        )


async def test_saver_rejects_write_copied_to_a_different_task_or_idx(tmp_path):
    # A write's real identity is finer than its checkpoint: two writes
    # under the same checkpoint still occupy distinct (task_id, idx)
    # paths (keys.write_key). One write's ciphertext relocated onto
    # another write's path within the same checkpoint must fail to
    # decrypt too.
    provider = _StaticKeyProvider()
    saver = make_saver(tmp_path, encryption=provider)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver._put_writes(stored, [("ch", "secret-for-A")], "task-A")

    src = keys.write_key(str(tmp_path), "t1", "", "ckpt-1", "task-A", 0)
    dst = keys.write_key(str(tmp_path), "t1", "", "ckpt-1", "task-B", 1)
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    shutil.copyfile(src, dst)

    with pytest.raises(InvalidTag):
        await saver._read_pending_writes("t1", "", "ckpt-1")


async def test_saver_rejects_unencrypted_checkpoint_when_encryption_configured(
    tmp_path,
):
    # A checkpoint written by a saver with no encryption= configured must
    # not be silently readable once encryption is turned on -- "encrypted
    # at rest" has to be enforced per object, not just for new writes.
    plain_saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await plain_saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    encrypted_saver = make_saver(tmp_path, encryption=_StaticKeyProvider())
    with pytest.raises(ValueError, match="never encrypted"):
        await encrypted_saver._get_tuple(config)


def test_key_provider_is_exported_from_package_root():
    from langgraph_checkpoint_objectstorage import KeyProvider

    assert isinstance(_StaticKeyProvider(), KeyProvider)

import uuid

import pytest
from cryptography.exceptions import InvalidTag

from langgraph_checkpoint_objectstorage import ObjectStorageSaver, keys


class _StaticKeyProvider:
    def __init__(self, key_id="k1", key=b"0" * 32):
        self.key_id = key_id
        self.key = key

    def get_key(self, thread_id, key_id=None):
        return self.key_id, self.key


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def _s3_saver(moto_s3_endpoint, s3_bucket, prefix=None, encryption=None):
    prefix = prefix or f"{s3_bucket}/{uuid.uuid4()}"
    return ObjectStorageSaver.from_conn_string(
        f"s3://{prefix}",
        key="testing",
        secret="testing",
        client_kwargs={"endpoint_url": moto_s3_endpoint},
        encryption=encryption,
    )


async def test_put_get_tuple_roundtrip_with_encryption_on_s3(
    moto_s3_endpoint, s3_bucket
):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, encryption=_StaticKeyProvider())
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "v" * 1000}
    await saver.aput(config, checkpoint, {"source": "input", "step": 0}, {})

    tup = await saver.aget_tuple(config)
    assert tup.checkpoint["channel_values"] == {"k": "v" * 1000}
    assert tup.metadata["source"] == "input"


async def test_put_writes_roundtrip_with_encryption_on_s3(moto_s3_endpoint, s3_bucket):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, encryption=_StaticKeyProvider())
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver.aput_writes(stored, [("ch", "val")], "task-1")

    tup = await saver.aget_tuple(config)
    assert tup.pending_writes == [("task-1", "ch", "val")]


async def test_encrypted_object_bytes_are_not_plaintext_on_s3(
    moto_s3_endpoint, s3_bucket
):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, encryption=_StaticKeyProvider())
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "a-very-recognizable-secret-value"}
    await saver.aput(config, checkpoint, {"step": 0}, {})

    key = keys.checkpoint_key(saver.root, "t1", "", "ckpt-1")
    raw = await saver._cat(key)
    assert b"a-very-recognizable-secret-value" not in raw


async def test_saver_rejects_checkpoint_copied_to_a_different_thread_on_s3(
    moto_s3_endpoint, s3_bucket
):
    provider = _StaticKeyProvider()
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, encryption=provider)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    src = keys.checkpoint_key(saver.root, "t1", "", "ckpt-1")
    dst = keys.checkpoint_key(saver.root, "t2", "", "ckpt-1")
    raw = await saver._cat(src)
    await saver._pipe(dst, raw)

    with pytest.raises(InvalidTag):
        await saver.aget_tuple(
            {"configurable": {"thread_id": "t2", "checkpoint_ns": ""}}
        )

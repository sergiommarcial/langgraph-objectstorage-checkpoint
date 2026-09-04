import asyncio
import uuid
from datetime import timedelta

from gcsfs.core import GCSFileSystem

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def _gcs_saver(fake_gcs_endpoint, gcs_compose_bucket, ttl):
    prefix = f"{gcs_compose_bucket}/{uuid.uuid4()}"
    # skip_instance_cache: each test needs its own GCSFileSystem, not one
    # reused (and loop-bound) from a previous test -- see the loop-binding
    # comment on gcs_compose_bucket in conftest.py.
    fs = GCSFileSystem(
        endpoint_url=fake_gcs_endpoint, token="anon", skip_instance_cache=True
    )
    return ObjectStorageSaver(fs, prefix, ttl=ttl)


async def test_delete_expired_removes_checkpoint_past_ttl_on_gcs(
    fake_gcs_endpoint, gcs_compose_bucket
):
    saver = _gcs_saver(fake_gcs_endpoint, gcs_compose_bucket, ttl=timedelta(seconds=1))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    # fake-gcs-server runs in its own container, whose clock can differ from
    # the test host's by a few ms -- a real sleep here (rather than a
    # ttl=0 edge case) keeps the object safely past its TTL regardless of
    # that skew.
    await asyncio.sleep(2)

    await saver.adelete_expired()

    tup = await saver.aget_tuple(config)
    assert tup is None


async def test_delete_expired_keeps_checkpoint_within_ttl_on_gcs(
    fake_gcs_endpoint, gcs_compose_bucket
):
    saver = _gcs_saver(fake_gcs_endpoint, gcs_compose_bucket, ttl=timedelta(days=1))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver.adelete_expired()

    tup = await saver.aget_tuple(config)
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"


async def test_delete_expired_ignores_sibling_prefix_in_shared_bucket_on_gcs(
    fake_gcs_endpoint, gcs_compose_bucket
):
    # GCS's matchesPrefix/list-with-prefix is a literal string match with
    # no directory-boundary awareness -- a naive `fs.find(root)` without
    # care could sweep a sibling object like "{root}-backup/..." in a
    # bucket shared with other data. This pins down that our own root/rm
    # bridge doesn't do that.
    saver = _gcs_saver(fake_gcs_endpoint, gcs_compose_bucket, ttl=timedelta(seconds=1))
    sibling_key = f"{saver.root}-backup/unrelated.txt"
    await saver.fs._pipe_file(sibling_key, b"do not touch")

    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await asyncio.sleep(2)

    await saver.adelete_expired()

    assert await saver.aget_tuple(config) is None
    assert await saver.fs._cat_file(sibling_key) == b"do not touch"

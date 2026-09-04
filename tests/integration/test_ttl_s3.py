import uuid
from datetime import timedelta

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


def _s3_saver(moto_s3_endpoint, s3_bucket, ttl):
    prefix = f"{s3_bucket}/{uuid.uuid4()}"
    return ObjectStorageSaver.from_conn_string(
        f"s3://{prefix}",
        key="testing",
        secret="testing",
        client_kwargs={"endpoint_url": moto_s3_endpoint},
        ttl=ttl,
    )


async def test_delete_expired_removes_checkpoint_past_ttl_on_s3(
    moto_s3_endpoint, s3_bucket
):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, ttl=timedelta(seconds=0))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver.adelete_expired()

    tup = await saver.aget_tuple(config)
    assert tup is None


async def test_delete_expired_keeps_checkpoint_within_ttl_on_s3(
    moto_s3_endpoint, s3_bucket
):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, ttl=timedelta(days=1))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver.adelete_expired()

    tup = await saver.aget_tuple(config)
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"


async def test_delete_expired_ignores_sibling_prefix_in_shared_bucket_on_s3(
    moto_s3_endpoint, s3_bucket
):
    # S3's ListObjectsV2 Prefix is a literal string match with no
    # directory-boundary awareness -- a naive `fs.find(root)` without care
    # could sweep a sibling key like "{root}-backup/..." in a bucket shared
    # with other data. This pins down that our own root/rm bridge doesn't
    # do that.
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, ttl=timedelta(seconds=0))
    sibling_key = f"{saver.root}-backup/unrelated.txt"
    await saver.fs._pipe_file(sibling_key, b"do not touch")

    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver.adelete_expired()

    assert await saver.aget_tuple(config) is None
    assert await saver.fs._cat_file(sibling_key) == b"do not touch"

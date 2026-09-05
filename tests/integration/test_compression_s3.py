import uuid

import pytest

from langgraph_checkpoint_objectstorage import ObjectStorageSaver, keys


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def _s3_saver(moto_s3_endpoint, s3_bucket, prefix=None, compression="none"):
    prefix = prefix or f"{s3_bucket}/{uuid.uuid4()}"
    return ObjectStorageSaver.from_conn_string(
        f"s3://{prefix}",
        key="testing",
        secret="testing",
        client_kwargs={"endpoint_url": moto_s3_endpoint},
        compression=compression,
    )


async def _object_size(saver: ObjectStorageSaver, key: str) -> int:
    detail = await saver._find_detailed(saver.root)
    return detail[key]["size"]


@pytest.mark.parametrize("compression", ["zlib", "lzma", "zstd"])
async def test_put_get_tuple_roundtrip_with_compression_on_s3(
    moto_s3_endpoint, s3_bucket, compression
):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, compression=compression)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "v" * 1000}
    await saver.aput(config, checkpoint, {"source": "input", "step": 0}, {})

    tup = await saver.aget_tuple(config)
    assert tup.checkpoint["channel_values"] == {"k": "v" * 1000}
    assert tup.metadata["source"] == "input"


async def test_put_writes_roundtrip_with_compression_on_s3(moto_s3_endpoint, s3_bucket):
    saver = _s3_saver(moto_s3_endpoint, s3_bucket, compression="zstd")
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver.aput_writes(stored, [("ch", "val")], "task-1")

    tup = await saver.aget_tuple(config)
    assert tup.pending_writes == [("task-1", "ch", "val")]


async def test_compressed_object_is_smaller_on_s3(moto_s3_endpoint, s3_bucket):
    # Object size on a real backend, read back via the S3 API itself
    # (not a local `os.path.getsize`) -- proves the bytes actually
    # uploaded are smaller, not just the bytes handed to `_pipe`.
    large_value = "x" * 100_000
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": large_value}

    none_saver = _s3_saver(moto_s3_endpoint, s3_bucket, compression="none")
    await none_saver.aput(config, checkpoint, {"step": 0}, {})
    none_key = keys.checkpoint_key(none_saver.root, "t1", "", "ckpt-1")
    none_size = await _object_size(none_saver, none_key)

    lzma_saver = _s3_saver(moto_s3_endpoint, s3_bucket, compression="lzma")
    await lzma_saver.aput(config, checkpoint, {"step": 0}, {})
    lzma_key = keys.checkpoint_key(lzma_saver.root, "t1", "", "ckpt-1")
    lzma_size = await _object_size(lzma_saver, lzma_key)

    assert lzma_size < none_size


async def test_saver_reads_checkpoint_written_under_a_different_codec_on_s3(
    moto_s3_endpoint, s3_bucket
):
    # A fleet reconfiguring `compression` between deploys must still read
    # what earlier deploys wrote to the same real bucket, under whichever
    # codec wrote it.
    prefix = f"{s3_bucket}/{uuid.uuid4()}"
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

    saver_lzma = _s3_saver(
        moto_s3_endpoint, s3_bucket, prefix=prefix, compression="lzma"
    )
    stored1 = await saver_lzma.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    saver_zlib = _s3_saver(
        moto_s3_endpoint, s3_bucket, prefix=prefix, compression="zlib"
    )
    config2 = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": stored1["configurable"]["checkpoint_id"],
        }
    }
    await saver_zlib.aput(config2, _checkpoint("ckpt-2"), {"step": 1}, {})

    tup_old = await saver_zlib.aget_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": "ckpt-1",
            }
        }
    )
    assert tup_old.checkpoint["id"] == "ckpt-1"
    tup_new = await saver_zlib.aget_tuple(config)
    assert tup_new.checkpoint["id"] == "ckpt-2"

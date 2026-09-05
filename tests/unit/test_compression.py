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

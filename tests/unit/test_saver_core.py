import asyncio
import gc
from concurrent.futures import ThreadPoolExecutor

import fsspec

from langgraph_checkpoint_objectstorage.saver import ObjectStorageSaver


def make_saver(tmp_path):
    fs = fsspec.filesystem("file")
    return ObjectStorageSaver(fs, str(tmp_path))


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


async def test_put_and_get_tuple_latest(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-1")
    checkpoint["channel_values"] = {"k": "v"}
    stored = await saver._put(config, checkpoint, {"source": "input", "step": 0}, {})
    assert stored["configurable"]["checkpoint_id"] == "ckpt-1"

    tup = await saver._get_tuple(
        {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    )
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"
    assert tup.checkpoint["channel_values"] == {"k": "v"}
    assert tup.metadata["source"] == "input"
    assert tup.parent_config is None


async def test_get_tuple_specific_checkpoint_id(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver._put(config, _checkpoint("ckpt-2"), {"step": 1}, {})

    tup = await saver._get_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": "ckpt-1",
            }
        }
    )
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"


async def test_put_chains_parent(tmp_path):
    saver = make_saver(tmp_path)
    config1 = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored1 = await saver._put(config1, _checkpoint("ckpt-1"), {"step": 0}, {})

    config2 = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": stored1["configurable"]["checkpoint_id"],
        }
    }
    stored2 = await saver._put(config2, _checkpoint("ckpt-2"), {"step": 1}, {})

    tup = await saver._get_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": stored2["configurable"]["checkpoint_id"],
            }
        }
    )
    assert tup.parent_config is not None
    assert tup.parent_config["configurable"]["checkpoint_id"] == "ckpt-1"


async def test_get_tuple_missing_thread_returns_none(tmp_path):
    saver = make_saver(tmp_path)
    tup = await saver._get_tuple(
        {"configurable": {"thread_id": "nope", "checkpoint_ns": ""}}
    )
    assert tup is None


async def test_get_tuple_missing_checkpoint_id_returns_none(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    tup = await saver._get_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": "nope",
            }
        }
    )
    assert tup is None


async def test_put_writes_idempotent_for_regular_channel(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver._put_writes(stored, [("ch", "val")], "task-1")
    await saver._put_writes(stored, [("ch", "val2")], "task-1")

    writes = await saver._read_pending_writes("t1", "", "ckpt-1")
    assert writes == [("task-1", "ch", "val")]


async def test_put_writes_special_channel_overwrites(tmp_path):
    from langgraph.checkpoint.serde.types import ERROR

    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver._put_writes(stored, [(ERROR, "first error")], "task-1")
    await saver._put_writes(stored, [(ERROR, "second error")], "task-1")

    writes = await saver._read_pending_writes("t1", "", "ckpt-1")
    assert writes == [("task-1", ERROR, "second error")]


async def test_read_pending_writes_empty_when_none(tmp_path):
    saver = make_saver(tmp_path)
    writes = await saver._read_pending_writes("t1", "", "ckpt-1")
    assert writes == []


def test_sync_calls_reuse_same_background_loop(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    first_loop = saver._bg_loop
    assert first_loop is not None

    saver.get_tuple(config)
    assert saver._bg_loop is first_loop


def test_concurrent_first_sync_calls_create_only_one_loop(tmp_path, monkeypatch):
    saver = make_saver(tmp_path)
    real_new_event_loop = asyncio.new_event_loop
    call_count = {"n": 0}

    def counting_new_event_loop():
        call_count["n"] += 1
        return real_new_event_loop()

    monkeypatch.setattr(asyncio, "new_event_loop", counting_new_event_loop)

    def do_put(i: int) -> None:
        config = {"configurable": {"thread_id": f"race-{i}", "checkpoint_ns": ""}}
        saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(do_put, range(8)))

    assert call_count["n"] == 1


def test_saver_gc_stops_background_thread(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    thread = saver._bg_thread
    assert thread.is_alive()

    del saver
    gc.collect()
    thread.join(timeout=2.0)
    assert not thread.is_alive()

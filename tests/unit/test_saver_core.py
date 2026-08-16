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

import fsspec

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def make_saver(tmp_path):
    return ObjectStorageSaver(fsspec.filesystem("file"), str(tmp_path))


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


# --- from_conn_string compression ---


def test_from_conn_string_compression_kwarg(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(
        f"file://{tmp_path}", compression="zlib"
    )
    assert saver._codec_name == "zlib"


def test_from_conn_string_compression_query_param(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(f"file://{tmp_path}?compression=lzma")
    assert saver._codec_name == "lzma"
    assert saver.root == str(tmp_path).rstrip("/")


def test_from_conn_string_compression_query_param_overrides_kwarg(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(
        f"file://{tmp_path}?compression=lzma", compression="zlib"
    )
    assert saver._codec_name == "lzma"


def test_from_conn_string_default_compression_is_none(tmp_path):
    saver = ObjectStorageSaver.from_conn_string(f"file://{tmp_path}")
    assert saver._codec_name is None


# --- sync API ---


def test_sync_put_get_list_roundtrip(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored1 = saver.put(
        config, _checkpoint("ckpt-1"), {"source": "input", "step": 0}, {}
    )
    config2 = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": stored1["configurable"]["checkpoint_id"],
        }
    }
    saver.put(config2, _checkpoint("ckpt-2"), {"source": "loop", "step": 1}, {})

    tup = saver.get_tuple({"configurable": {"thread_id": "t1", "checkpoint_ns": ""}})
    assert tup.checkpoint["id"] == "ckpt-2"

    results = list(
        saver.list({"configurable": {"thread_id": "t1", "checkpoint_ns": ""}})
    )
    assert [t.checkpoint["id"] for t in results] == ["ckpt-2", "ckpt-1"]


def test_sync_list_before_and_limit(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    saver.put(config, _checkpoint("ckpt-2"), {"step": 1}, {})
    saver.put(config, _checkpoint("ckpt-3"), {"step": 2}, {})

    before = {"configurable": {"checkpoint_id": "ckpt-3"}}
    results = list(
        saver.list(
            {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}},
            before=before,
            limit=1,
        )
    )
    assert [t.checkpoint["id"] for t in results] == ["ckpt-2"]


def test_sync_list_metadata_filter(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-1"), {"source": "input", "step": 0}, {})
    saver.put(config, _checkpoint("ckpt-2"), {"source": "loop", "step": 1}, {})

    results = list(
        saver.list(
            {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}},
            filter={"source": "loop"},
        )
    )
    assert len(results) == 1
    assert results[0].checkpoint["id"] == "ckpt-2"


def test_sync_put_writes_and_delete_thread(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    saver.put_writes(stored, [("ch", "val")], "task-1")

    tup = saver.get_tuple({"configurable": {"thread_id": "t1", "checkpoint_ns": ""}})
    assert tup.pending_writes == [("task-1", "ch", "val")]

    saver.delete_thread("t1")
    assert (
        saver.get_tuple({"configurable": {"thread_id": "t1", "checkpoint_ns": ""}})
        is None
    )


# --- async API ---


async def test_async_put_get_list_roundtrip(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored1 = await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    config2 = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": stored1["configurable"]["checkpoint_id"],
        }
    }
    await saver.aput(config2, _checkpoint("ckpt-2"), {"step": 1}, {})

    tup = await saver.aget_tuple(
        {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    )
    assert tup.checkpoint["id"] == "ckpt-2"

    results = [
        t
        async for t in saver.alist(
            {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
        )
    ]
    assert [t.checkpoint["id"] for t in results] == ["ckpt-2", "ckpt-1"]


async def test_async_put_writes_and_delete_thread(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver.aput_writes(stored, [("ch", "val")], "task-1")

    tup = await saver.aget_tuple(
        {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    )
    assert tup.pending_writes == [("task-1", "ch", "val")]

    await saver.adelete_thread("t1")
    tup = await saver.aget_tuple(
        {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    )
    assert tup is None


async def test_delete_thread_removes_all_namespaces(tmp_path):
    saver = make_saver(tmp_path)
    for ns in ["", "child:1"]:
        cfg = {"configurable": {"thread_id": "t1", "checkpoint_ns": ns}}
        await saver.aput(cfg, _checkpoint(f"ckpt-{ns or 'root'}"), {"step": 0}, {})

    await saver.adelete_thread("t1")

    for ns in ["", "child:1"]:
        cfg = {"configurable": {"thread_id": "t1", "checkpoint_ns": ns}}
        assert await saver.aget_tuple(cfg) is None


# --- export/import ---


def test_sync_export_import_thread_round_trip(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    packed = saver.export_thread("t1")
    saver.delete_thread("t1")
    assert saver.get_tuple(config) is None

    saver.import_thread(packed)

    tup = saver.get_tuple(config)
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"


async def test_async_export_import_thread_round_trip(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    packed = await saver.aexport_thread("t1")
    await saver.adelete_thread("t1")

    await saver.aimport_thread(packed)

    tup = await saver.aget_tuple(config)
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"

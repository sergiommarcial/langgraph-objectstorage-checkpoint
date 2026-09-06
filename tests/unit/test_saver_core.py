import asyncio
import gc
from concurrent.futures import ThreadPoolExecutor

import fsspec
import pytest

from langgraph_checkpoint_objectstorage import archive, keys
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


# --- export/import ---


async def test_export_thread_covers_all_namespaces(tmp_path):
    saver = make_saver(tmp_path)
    for ns in ["", "child:1"]:
        config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ns}}
        await saver._put(config, _checkpoint(f"ckpt-{ns or 'root'}"), {"step": 0}, {})

    packed = await saver._export_thread("t1")
    entries = archive.unpack(packed)

    root = str(tmp_path)
    expected_paths = {
        keys.checkpoint_key(root, "t1", ns, f"ckpt-{ns or 'root'}")[len(root) + 1 :]
        for ns in ["", "child:1"]
    }
    assert set(entries) == expected_paths


async def test_export_thread_missing_thread_raises_keyerror(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(KeyError):
        await saver._export_thread("nope")


async def test_export_import_thread_round_trip_restores_full_history(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored1 = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    config2 = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": stored1["configurable"]["checkpoint_id"],
        }
    }
    stored2 = await saver._put(config2, _checkpoint("ckpt-2"), {"step": 1}, {})
    await saver._put_writes(stored2, [("ch", "val")], "task-1")

    packed = await saver._export_thread("t1")
    await saver._delete_thread("t1")
    assert await saver._get_tuple(config) is None

    await saver._import_thread(packed)

    tup = await saver._get_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": "ckpt-2",
            }
        }
    )
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-2"
    assert tup.parent_config["configurable"]["checkpoint_id"] == "ckpt-1"
    assert tup.pending_writes == [("task-1", "ch", "val")]


async def test_import_thread_with_dest_thread_id_renames(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    packed = await saver._export_thread("t1")
    await saver._import_thread(packed, dest_thread_id="t2")

    renamed = await saver._get_tuple(
        {"configurable": {"thread_id": "t2", "checkpoint_ns": ""}}
    )
    assert renamed is not None
    assert renamed.checkpoint["id"] == "ckpt-1"

    original = await saver._get_tuple(config)
    assert original is not None
    assert original.checkpoint["id"] == "ckpt-1"


async def test_import_thread_overwrite_false_writes_nothing_on_conflict(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver._put(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": "ckpt-1",
            }
        },
        _checkpoint("ckpt-2"),
        {"step": 1},
        {},
    )
    packed = await saver._export_thread("t1")

    conflict_key = keys.checkpoint_key(str(tmp_path), "t2", "", "ckpt-1")
    await saver._pipe(conflict_key, b"dummy")

    with pytest.raises(FileExistsError):
        await saver._import_thread(packed, dest_thread_id="t2", overwrite=False)

    dest_prefix = keys.thread_prefix(str(tmp_path), "t2")
    assert await saver._find(dest_prefix) == [conflict_key]
    assert await saver._cat(conflict_key) == b"dummy"


async def test_import_thread_overwrite_true_replaces_existing(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    packed = await saver._export_thread("t1")

    conflict_key = keys.checkpoint_key(str(tmp_path), "t2", "", "ckpt-1")
    await saver._pipe(conflict_key, b"dummy")

    await saver._import_thread(packed, dest_thread_id="t2", overwrite=True)

    assert await saver._cat(conflict_key) != b"dummy"
    tup = await saver._get_tuple(
        {"configurable": {"thread_id": "t2", "checkpoint_ns": ""}}
    )
    assert tup.checkpoint["id"] == "ckpt-1"


async def test_import_thread_multiple_thread_ids_in_archive_raises(tmp_path):
    saver = make_saver(tmp_path)
    bad_archive = archive.pack(
        {
            "t1/checkpoints/ckpt-1.msgpack": b"x",
            "t2/checkpoints/ckpt-1.msgpack": b"y",
        }
    )
    with pytest.raises(ValueError, match="distinct thread_id"):
        await saver._import_thread(bad_archive)


async def test_import_thread_empty_archive_raises(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="distinct thread_id"):
        await saver._import_thread(archive.pack({}))


async def test_export_thread_rejects_thread_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._export_thread("team/proj-1")


async def test_import_thread_rejects_dest_thread_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    packed = await saver._export_thread("t1")

    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._import_thread(packed, dest_thread_id="team/proj-1")


async def test_import_thread_rejects_path_traversal_archive(tmp_path):
    saver = make_saver(tmp_path)
    malicious = archive.pack({"../escape.txt": b"PWNED"})

    with pytest.raises(ValueError, match="unsafe path"):
        await saver._import_thread(malicious)

    assert not (tmp_path.parent / "escape.txt").exists()


# --- thread_id/checkpoint_ns validation ---


async def test_put_rejects_thread_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "team/proj-1", "checkpoint_ns": ""}}
    with pytest.raises(ValueError, match="thread_id"):
        await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})


async def test_put_rejects_checkpoint_ns_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": "a/b"}}
    with pytest.raises(ValueError, match="checkpoint_ns"):
        await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})


async def test_get_tuple_rejects_thread_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="thread_id"):
        await saver._get_tuple(
            {"configurable": {"thread_id": "team/proj-1", "checkpoint_ns": ""}}
        )


async def test_delete_thread_rejects_thread_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="thread_id"):
        await saver._delete_thread("team/proj-1")


async def test_put_rejects_checkpoint_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    with pytest.raises(ValueError, match="checkpoint_id"):
        await saver._put(config, _checkpoint("../evil"), {"step": 0}, {})


async def test_get_tuple_rejects_checkpoint_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="checkpoint_id"):
        await saver._get_tuple(
            {
                "configurable": {
                    "thread_id": "t1",
                    "checkpoint_ns": "",
                    "checkpoint_id": "../evil",
                }
            }
        )


async def test_put_writes_rejects_checkpoint_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    config = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": "../evil",
        }
    }
    with pytest.raises(ValueError, match="checkpoint_id"):
        await saver._put_writes(config, [("ch", "val")], "task-1")


async def test_put_writes_rejects_task_id_containing_slash(tmp_path):
    saver = make_saver(tmp_path)
    config = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": "ckpt-1",
        }
    }
    with pytest.raises(ValueError, match="task_id"):
        await saver._put_writes(config, [("ch", "val")], "../evil-task")


async def test_import_thread_rejects_bare_entry_with_no_nested_path(tmp_path):
    saver = make_saver(tmp_path)
    bad_archive = archive.pack({"t1": b"data"})
    with pytest.raises(ValueError, match="not a thread-relative key"):
        await saver._import_thread(bad_archive)


async def test_import_thread_rejects_empty_dest_thread_id(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    packed = await saver._export_thread("t1")

    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._import_thread(packed, dest_thread_id="")


async def test_delete_thread_rejects_parent_directory_thread_id(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._delete_thread("..")


async def test_put_writes_rejects_parent_directory_task_id(tmp_path):
    saver = make_saver(tmp_path)
    config = {
        "configurable": {
            "thread_id": "t1",
            "checkpoint_ns": "",
            "checkpoint_id": "ckpt-1",
        }
    }
    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._put_writes(config, [("ch", "val")], "..")


async def test_export_thread_rejects_current_directory_thread_id(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._export_thread(".")


async def test_put_rejects_parent_directory_checkpoint_ns(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ".."}}
    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})


async def test_put_rejects_backslash_in_thread_id(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "..\\..\\evil", "checkpoint_ns": ""}}
    with pytest.raises(ValueError, match="not a valid path segment"):
        await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})


async def test_import_thread_rejects_backslash_in_archive_entry_path(tmp_path):
    saver = make_saver(tmp_path)
    malicious = archive.pack({"evil\\..\\..\\pwned/checkpoints/ckpt-1.msgpack": b"x"})
    with pytest.raises(ValueError, match="unsafe path"):
        await saver._import_thread(malicious)

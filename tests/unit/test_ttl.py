import logging
import os
from datetime import timedelta

import fsspec

from langgraph_checkpoint_objectstorage import keys
from langgraph_checkpoint_objectstorage.saver import ObjectStorageSaver


def make_saver(root, ttl=None):
    fs = fsspec.filesystem("file")
    return ObjectStorageSaver(fs, str(root), ttl=ttl)


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def _backdate(path: str, seconds_ago: float) -> None:
    now = os.stat(path).st_mtime
    target = now - seconds_ago
    os.utime(path, (target, target))


async def test_delete_expired_removes_checkpoint_and_writes_older_than_ttl(tmp_path):
    saver = make_saver(tmp_path, ttl=timedelta(seconds=60))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver._put_writes(stored, [("ch", "val")], "task-1")

    ckpt_path = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    write_path = keys.write_key(str(tmp_path), "t1", "", "ckpt-1", "task-1", 0)
    _backdate(ckpt_path, seconds_ago=120)
    _backdate(write_path, seconds_ago=120)

    await saver._delete_expired()

    assert await saver._get_tuple(config) is None
    assert not saver.fs.exists(ckpt_path)
    assert not saver.fs.exists(write_path)


async def test_delete_expired_keeps_checkpoint_within_ttl(tmp_path):
    saver = make_saver(tmp_path, ttl=timedelta(days=1))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})

    await saver._delete_expired()

    tup = await saver._get_tuple(config)
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-1"


async def test_delete_expired_noop_when_ttl_none(tmp_path):
    saver = make_saver(tmp_path, ttl=None)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    ckpt_path = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    _backdate(ckpt_path, seconds_ago=10_000)

    await saver._delete_expired()

    assert await saver._get_tuple(config) is not None


async def test_delete_expired_boundary_keeps_object_newer_than_cutoff(tmp_path):
    saver = make_saver(tmp_path, ttl=timedelta(seconds=60))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    ckpt_path = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    _backdate(ckpt_path, seconds_ago=59)

    await saver._delete_expired()

    assert await saver._get_tuple(config) is not None


async def test_delete_expired_boundary_removes_object_older_than_cutoff(tmp_path):
    saver = make_saver(tmp_path, ttl=timedelta(seconds=60))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    ckpt_path = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    _backdate(ckpt_path, seconds_ago=61)

    await saver._delete_expired()

    assert await saver._get_tuple(config) is None


async def test_delete_expired_ignores_sibling_prefix_in_shared_bucket(tmp_path):
    # A bucket/directory shared with unrelated data whose name happens to
    # start with the same string as `root` (e.g. "checkpoints-backup" next
    # to "checkpoints") must never be swept by a saver scoped to `root`.
    root = tmp_path / "checkpoints"
    sibling = tmp_path / "checkpoints-backup"
    sibling.mkdir()
    sibling_file = sibling / "unrelated.txt"
    sibling_file.write_text("do not touch")
    _backdate(str(sibling_file), seconds_ago=120)

    saver = make_saver(root, ttl=timedelta(seconds=60))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver._put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    ckpt_path = keys.checkpoint_key(str(root), "t1", "", "ckpt-1")
    _backdate(ckpt_path, seconds_ago=120)

    await saver._delete_expired()

    assert await saver._get_tuple(config) is None
    assert sibling_file.exists()
    assert sibling_file.read_text() == "do not touch"


async def test_delete_expired_missing_root_is_noop(tmp_path):
    missing_root = tmp_path / "does-not-exist"
    saver = make_saver(missing_root, ttl=timedelta(seconds=1))
    await saver._delete_expired()  # must not raise


def test_sync_delete_expired_removes_expired_checkpoint(tmp_path):
    saver = make_saver(tmp_path, ttl=timedelta(seconds=60))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    ckpt_path = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    _backdate(ckpt_path, seconds_ago=120)

    saver.delete_expired()

    assert saver.get_tuple(config) is None


async def test_async_adelete_expired_removes_expired_checkpoint(tmp_path):
    saver = make_saver(tmp_path, ttl=timedelta(seconds=60))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    ckpt_path = keys.checkpoint_key(str(tmp_path), "t1", "", "ckpt-1")
    _backdate(ckpt_path, seconds_ago=120)

    await saver.adelete_expired()

    assert await saver.aget_tuple(config) is None


def test_ttl_set_warns_that_saver_does_not_enforce_it(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="langgraph_checkpoint_objectstorage")
    make_saver(tmp_path, ttl=timedelta(days=1))

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "delete_expired" in warnings[0].message
    assert "lifecycle rule" in warnings[0].message


def test_no_ttl_does_not_warn(tmp_path, caplog):
    caplog.set_level(logging.WARNING, logger="langgraph_checkpoint_objectstorage")
    make_saver(tmp_path)

    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warnings == []

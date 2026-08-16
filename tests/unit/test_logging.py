import logging

import fsspec
import pytest

from langgraph_checkpoint_objectstorage import ObjectStorageSaver
from langgraph_checkpoint_objectstorage.saver import _LOG_LEVEL_ENV, logger


@pytest.fixture(autouse=True)
def _reset_logger_level():
    original = logger.level
    yield
    logger.setLevel(original)


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def test_env_var_sets_log_level(tmp_path, monkeypatch):
    monkeypatch.setenv(_LOG_LEVEL_ENV, "DEBUG")
    ObjectStorageSaver(fsspec.filesystem("file"), str(tmp_path))
    assert logger.level == logging.DEBUG


def test_no_env_var_leaves_level_untouched(tmp_path, monkeypatch):
    monkeypatch.delenv(_LOG_LEVEL_ENV, raising=False)
    logger.setLevel(logging.WARNING)
    ObjectStorageSaver(fsspec.filesystem("file"), str(tmp_path))
    assert logger.level == logging.WARNING


async def test_debug_level_surfaces_io_during_put_and_get(tmp_path, caplog):
    caplog.set_level(logging.DEBUG, logger="langgraph_checkpoint_objectstorage")
    saver = ObjectStorageSaver(fsspec.filesystem("file"), str(tmp_path))
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

    await saver.aput(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    await saver.aget_tuple({"configurable": {"thread_id": "t1", "checkpoint_ns": ""}})

    debug_records = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert len(debug_records) >= 2

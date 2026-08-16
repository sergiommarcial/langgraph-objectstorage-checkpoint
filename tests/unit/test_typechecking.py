from collections import ChainMap

import fsspec
import pytest
from typeguard import TypeCheckError

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
        # real langgraph checkpoints carry this legacy field, which isn't
        # part of the Checkpoint TypedDict declaration
        "pending_sends": [],
    }


def test_delete_thread_rejects_non_string_thread_id(tmp_path):
    saver = make_saver(tmp_path)
    with pytest.raises(TypeCheckError):
        saver.delete_thread(12345)


def test_put_writes_rejects_non_sequence_writes(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    with pytest.raises(TypeCheckError):
        saver.put_writes(stored, "not-a-sequence-of-tuples", "task-1")


def test_put_accepts_real_checkpoint_shape_with_legacy_extra_field(tmp_path):
    saver = make_saver(tmp_path)
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}
    stored = saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    assert stored["configurable"]["checkpoint_id"] == "ckpt-1"


def test_put_accepts_real_config_shape_with_chainmap_metadata(tmp_path):
    # Real langgraph invocations pass a RunnableConfig whose "metadata"
    # field is a collections.ChainMap, not a plain dict -- this regression
    # test is what actually running the README quickstart against a real
    # graph.invoke() caught: typeguard rejected every real invocation
    # before config was loosened to Mapping[str, Any].
    saver = make_saver(tmp_path)
    config = {
        "configurable": {"thread_id": "t1", "checkpoint_ns": ""},
        "metadata": ChainMap({"thread_id": "t1"}),
        "tags": [],
        "callbacks": None,
        "recursion_limit": 25,
    }
    stored = saver.put(config, _checkpoint("ckpt-1"), {"step": 0}, {})
    assert stored["configurable"]["checkpoint_id"] == "ckpt-1"

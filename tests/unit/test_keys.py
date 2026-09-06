import pytest

from langgraph_checkpoint_objectstorage import keys


def test_checkpoint_key_under_prefix():
    key = keys.checkpoint_key("root", "thread-1", "", "ckpt-1")
    prefix = keys.checkpoints_prefix("root", "thread-1", "")
    assert key.startswith(prefix)
    assert key == prefix + "ckpt-1.msgpack"


def test_checkpoint_key_with_namespace_no_double_slash():
    key = keys.checkpoint_key("root", "thread-1", "child:1", "ckpt-1")
    assert "//" not in key
    assert key == "root/thread-1/child:1/checkpoints/ckpt-1.msgpack"


def test_checkpoint_id_from_key_round_trips():
    key = keys.checkpoint_key("root", "thread-1", "", "ckpt-1")
    assert keys.checkpoint_id_from_key(key) == "ckpt-1"


def test_write_key_under_writes_prefix():
    key = keys.write_key("root", "thread-1", "", "ckpt-1", "task-1", 0)
    prefix = keys.writes_prefix("root", "thread-1", "", "ckpt-1")
    assert key.startswith(prefix)
    assert key == prefix + "task-1/0.msgpack"


def test_write_key_negative_idx():
    key = keys.write_key("root", "thread-1", "", "ckpt-1", "task-1", -1)
    assert key.endswith("task-1/-1.msgpack")


def test_write_task_id_and_idx_from_key_round_trips():
    key = keys.write_key("root", "thread-1", "", "ckpt-1", "task-1", 3)
    assert keys.write_task_id_and_idx_from_key(key) == ("task-1", 3)


def test_write_task_id_and_idx_from_key_negative_idx():
    key = keys.write_key("root", "thread-1", "", "ckpt-1", "task-1", -1)
    assert keys.write_task_id_and_idx_from_key(key) == ("task-1", -1)


def test_thread_prefix_covers_all_namespaces():
    tp = keys.thread_prefix("root", "thread-1")
    for ns in ["", "child:1", "child:2"]:
        assert keys.checkpoints_prefix("root", "thread-1", ns).startswith(tp)
        assert keys.writes_prefix("root", "thread-1", ns, "ckpt-1").startswith(tp)


def test_thread_prefix_distinct_threads_not_prefixes_of_each_other():
    assert not keys.thread_prefix("root", "thread-1").startswith(
        keys.thread_prefix("root", "thread-12")
    )


def test_thread_id_from_relative_key_takes_first_segment():
    key = keys.checkpoint_key("root", "thread-1", "", "ckpt-1")
    relative = key[len("root") + 1 :]
    assert keys.thread_id_from_relative_key(relative) == "thread-1"


def test_thread_id_from_relative_key_rejects_bare_segment_with_no_subpath():
    with pytest.raises(ValueError, match="not a thread-relative key"):
        keys.thread_id_from_relative_key("thread-1")


def test_relative_key_strips_root_prefix():
    key = keys.checkpoint_key("root", "thread-1", "", "ckpt-1")
    assert keys.relative_key("root", key) == "thread-1/checkpoints/ckpt-1.msgpack"


def test_rekeyed_path_rewrites_thread_id_segment():
    relative = "thread-1/checkpoints/ckpt-1.msgpack"
    assert (
        keys.rekeyed_path("root", relative, "thread-2")
        == "root/thread-2/checkpoints/ckpt-1.msgpack"
    )


def test_is_safe_segment_allows_common_identifier_characters():
    assert keys.is_safe_segment("thread-1")
    assert keys.is_safe_segment("child:1")
    assert keys.is_safe_segment("ckpt_1.v2")
    assert keys.is_safe_segment("café")


def test_is_safe_segment_rejects_empty_and_traversal_segments():
    assert not keys.is_safe_segment("")
    assert not keys.is_safe_segment(".")
    assert not keys.is_safe_segment("..")


def test_is_safe_segment_rejects_path_separators():
    assert not keys.is_safe_segment("a/b")
    assert not keys.is_safe_segment("a\\b")


def test_is_safe_segment_rejects_characters_outside_the_allowlist():
    assert not keys.is_safe_segment("evil\x00null")
    assert not keys.is_safe_segment("evil*star")
    assert not keys.is_safe_segment("evil\nline")

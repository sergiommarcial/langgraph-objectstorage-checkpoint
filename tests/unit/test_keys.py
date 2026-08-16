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


def test_thread_prefix_covers_all_namespaces():
    tp = keys.thread_prefix("root", "thread-1")
    for ns in ["", "child:1", "child:2"]:
        assert keys.checkpoints_prefix("root", "thread-1", ns).startswith(tp)
        assert keys.writes_prefix("root", "thread-1", ns, "ckpt-1").startswith(tp)


def test_thread_prefix_distinct_threads_not_prefixes_of_each_other():
    assert not keys.thread_prefix("root", "thread-1").startswith(
        keys.thread_prefix("root", "thread-12")
    )

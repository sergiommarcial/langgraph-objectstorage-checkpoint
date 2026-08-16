from langgraph_checkpoint_objectstorage import envelope


def test_pack_unpack_checkpoint_round_trip():
    checkpoint = {
        "v": 1,
        "id": "ckpt-1",
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {"k": "v", "n": 42},
        "channel_versions": {"k": 1},
        "versions_seen": {"node": {"k": 1}},
    }
    metadata = {"source": "input", "step": 0, "parents": {}}
    data = envelope.pack_checkpoint(checkpoint, metadata, None)
    out_checkpoint, out_metadata, parent_id = envelope.unpack_checkpoint(data)
    assert out_checkpoint == checkpoint
    assert out_metadata == metadata
    assert parent_id is None


def test_pack_unpack_checkpoint_with_parent():
    data = envelope.pack_checkpoint({"v": 1, "id": "c2"}, {"step": 1}, "c1")
    _, _, parent_id = envelope.unpack_checkpoint(data)
    assert parent_id == "c1"


def test_pack_unpack_write_round_trip():
    data = envelope.pack_write("task-1", 0, "my_channel", {"nested": [1, 2, 3]})
    task_id, idx, channel, value = envelope.unpack_write(data)
    assert task_id == "task-1"
    assert idx == 0
    assert channel == "my_channel"
    assert value == {"nested": [1, 2, 3]}


def test_pack_unpack_write_negative_idx():
    data = envelope.pack_write("task-1", -1, "__error__", "boom")
    _, idx, _, _ = envelope.unpack_write(data)
    assert idx == -1

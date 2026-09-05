import ormsgpack
import pytest

from langgraph_checkpoint_objectstorage import envelope


def test_resolve_codec_none_returns_none():
    assert envelope.resolve_codec("none") is None


@pytest.mark.parametrize("name", ["zlib", "lzma", "zstd"])
def test_resolve_codec_known_name_returns_itself(name):
    assert envelope.resolve_codec(name) == name


def test_resolve_codec_unknown_name_raises_value_error():
    with pytest.raises(ValueError, match="bogus"):
        envelope.resolve_codec("bogus")


def test_resolve_codec_zstd_without_extra_raises_import_error(monkeypatch):
    monkeypatch.delitem(envelope._CODECS, "zstd", raising=False)
    with pytest.raises(ImportError, match="zstd"):
        envelope.resolve_codec("zstd")


@pytest.mark.parametrize("codec_name", ["zlib", "lzma", "zstd"])
def test_pack_unpack_checkpoint_round_trip_with_compression(codec_name):
    checkpoint = {
        "v": 1,
        "id": "ckpt-1",
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {"k": "v" * 1000},
        "channel_versions": {"k": 1},
        "versions_seen": {"node": {"k": 1}},
    }
    metadata = {"source": "input", "step": 0, "parents": {}}
    data = envelope.pack_checkpoint(checkpoint, metadata, None, codec_name=codec_name)
    out_checkpoint, out_metadata, parent_id = envelope.unpack_checkpoint(data)
    assert out_checkpoint == checkpoint
    assert out_metadata == metadata
    assert parent_id is None


def test_pack_checkpoint_none_codec_matches_legacy_uncompressed_shape():
    data = envelope.pack_checkpoint({"v": 1, "id": "c1"}, {"step": 0}, None)
    obj = ormsgpack.unpackb(data)
    assert set(obj.keys()) == {"checkpoint", "metadata", "parent_checkpoint_id"}


def test_unpack_checkpoint_reads_legacy_object_with_no_wrapper():
    # Simulates an object written before compression existed: no "codec"
    # wrapper at all, just the inner packed dict directly.
    ck_type, ck_bytes = envelope._serde.dumps_typed({"v": 1, "id": "c1"})
    md_type, md_bytes = envelope._serde.dumps_typed({"step": 0})
    legacy = ormsgpack.packb(
        {
            "checkpoint": [ck_type, ck_bytes],
            "metadata": [md_type, md_bytes],
            "parent_checkpoint_id": None,
        }
    )
    checkpoint, metadata, parent_id = envelope.unpack_checkpoint(legacy)
    assert checkpoint == {"v": 1, "id": "c1"}
    assert metadata == {"step": 0}
    assert parent_id is None


def test_unpack_checkpoint_rejects_unknown_codec():
    bogus = ormsgpack.packb({"codec": "brotli", "payload": b"whatever"})
    with pytest.raises(ValueError, match="brotli"):
        envelope.unpack_checkpoint(bogus)


@pytest.mark.parametrize("codec_name", ["zlib", "lzma", "zstd"])
def test_pack_unpack_write_round_trip_with_compression(codec_name):
    data = envelope.pack_write(
        "task-1", 0, "my_channel", {"nested": [1, 2, 3]}, codec_name=codec_name
    )
    task_id, idx, channel, value = envelope.unpack_write(data)
    assert task_id == "task-1"
    assert idx == 0
    assert channel == "my_channel"
    assert value == {"nested": [1, 2, 3]}


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

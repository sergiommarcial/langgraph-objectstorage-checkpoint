import ormsgpack
import pytest
from cryptography.exceptions import InvalidTag

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


class _StaticKeyProvider:
    def __init__(self, key_id: str = "k1", key: bytes = b"0" * 32):
        self.key_id = key_id
        self.key = key

    def get_key(self, thread_id, key_id=None):
        return self.key_id, self.key


class _RotatingKeyProvider:
    def __init__(self):
        self._keys = {"k1": b"1" * 32}
        self._current = "k1"

    def rotate(self, new_key_id: str, new_key: bytes) -> None:
        self._keys[new_key_id] = new_key
        self._current = new_key_id

    def get_key(self, thread_id, key_id=None):
        resolved = key_id or self._current
        return resolved, self._keys[resolved]


def test_pack_unpack_checkpoint_round_trip_with_encryption():
    provider = _StaticKeyProvider()
    checkpoint = {"v": 1, "id": "ckpt-1", "channel_values": {"k": "v" * 1000}}
    metadata = {"source": "input", "step": 0}
    data = envelope.pack_checkpoint(
        checkpoint,
        metadata,
        None,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="ckpt-1",
        key_provider=provider,
    )
    out_checkpoint, out_metadata, parent_id = envelope.unpack_checkpoint(
        data,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="ckpt-1",
        key_provider=provider,
    )
    assert out_checkpoint == checkpoint
    assert out_metadata == metadata
    assert parent_id is None


def test_pack_checkpoint_records_enc_and_key_id_fields():
    provider = _StaticKeyProvider(key_id="k-42")
    data = envelope.pack_checkpoint(
        {"v": 1, "id": "c1"},
        {"step": 0},
        None,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    obj = ormsgpack.unpackb(data)
    assert obj["enc"] == "aes-256-gcm"
    assert obj["key_id"] == "k-42"


def test_compress_then_encrypt_pipeline_order():
    # Compress-then-encrypt on write: the outer envelope must be the
    # encryption layer, with the compressed bytes as its ciphertext --
    # encrypting first would make the payload high-entropy and defeat
    # compression entirely.
    provider = _StaticKeyProvider()
    data = envelope.pack_checkpoint(
        {"v": 1, "id": "c1", "channel_values": {"k": "v" * 1000}},
        {"step": 0},
        None,
        codec_name="zlib",
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    outer = ormsgpack.unpackb(data)
    assert outer["enc"] == "aes-256-gcm"
    inner = envelope._decrypt_layer(outer, "t1", "", "c1", provider)
    inner_obj = ormsgpack.unpackb(inner)
    assert inner_obj["codec"] == "zlib"

    out_checkpoint, _, _ = envelope.unpack_checkpoint(
        data,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    assert out_checkpoint["channel_values"] == {"k": "v" * 1000}


def test_unpack_checkpoint_reads_checkpoint_written_under_a_rotated_key():
    provider = _RotatingKeyProvider()
    data1 = envelope.pack_checkpoint(
        {"v": 1, "id": "c1"},
        {"step": 0},
        None,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    provider.rotate("k2", b"2" * 32)
    data2 = envelope.pack_checkpoint(
        {"v": 1, "id": "c2"},
        {"step": 1},
        "c1",
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c2",
        key_provider=provider,
    )

    ck1, _, _ = envelope.unpack_checkpoint(
        data1,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    ck2, _, parent = envelope.unpack_checkpoint(
        data2,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c2",
        key_provider=provider,
    )
    assert ck1["id"] == "c1"
    assert ck2["id"] == "c2"
    assert parent == "c1"


def test_decrypt_fails_when_object_moved_to_a_different_thread():
    # AAD binds ciphertext to thread_id: an object copied to a different
    # thread's path must fail to decrypt, not silently decrypt under
    # whatever key that path happens to resolve to.
    provider = _StaticKeyProvider()
    data = envelope.pack_checkpoint(
        {"v": 1, "id": "c1"},
        {"step": 0},
        None,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    with pytest.raises(InvalidTag):
        envelope.unpack_checkpoint(
            data,
            thread_id="t2",
            checkpoint_ns="",
            checkpoint_id="c1",
            key_provider=provider,
        )


def test_decrypt_fails_when_object_moved_to_a_different_checkpoint():
    # AAD also binds checkpoint_id: an object swapped onto a different
    # checkpoint under the *same* thread must fail to decrypt too, not
    # just a cross-thread move.
    provider = _StaticKeyProvider()
    data = envelope.pack_checkpoint(
        {"v": 1, "id": "c1"},
        {"step": 0},
        None,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    with pytest.raises(InvalidTag):
        envelope.unpack_checkpoint(
            data,
            thread_id="t1",
            checkpoint_ns="",
            checkpoint_id="c2",
            key_provider=provider,
        )


def test_decrypt_fails_when_object_moved_to_a_different_namespace():
    # Same idea, for checkpoint_ns.
    provider = _StaticKeyProvider()
    data = envelope.pack_checkpoint(
        {"v": 1, "id": "c1"},
        {"step": 0},
        None,
        thread_id="t1",
        checkpoint_ns="ns1",
        checkpoint_id="c1",
        key_provider=provider,
    )
    with pytest.raises(InvalidTag):
        envelope.unpack_checkpoint(
            data,
            thread_id="t1",
            checkpoint_ns="ns2",
            checkpoint_id="c1",
            key_provider=provider,
        )


def test_pack_unpack_write_round_trip_with_encryption():
    provider = _StaticKeyProvider()
    data = envelope.pack_write(
        "task-1",
        0,
        "my_channel",
        {"nested": [1, 2, 3]},
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    task_id, idx, channel, value = envelope.unpack_write(
        data,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        task_id="task-1",
        idx=0,
        key_provider=provider,
    )
    assert task_id == "task-1"
    assert idx == 0
    assert channel == "my_channel"
    assert value == {"nested": [1, 2, 3]}


def test_decrypt_fails_when_write_moved_to_a_different_task_or_idx():
    # A write's real identity is finer than its checkpoint: two writes
    # under the same checkpoint still occupy distinct (task_id, idx)
    # paths. One write's ciphertext relocated onto another write's path
    # within the same checkpoint must fail to decrypt too.
    provider = _StaticKeyProvider()
    data = envelope.pack_write(
        "task-A",
        0,
        "ch",
        "secret",
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    with pytest.raises(InvalidTag):
        envelope.unpack_write(
            data,
            thread_id="t1",
            checkpoint_ns="",
            checkpoint_id="c1",
            task_id="task-B",
            idx=1,
            key_provider=provider,
        )


def test_unpack_checkpoint_raises_when_encrypted_but_no_key_provider_configured():
    provider = _StaticKeyProvider()
    data = envelope.pack_checkpoint(
        {"v": 1, "id": "c1"},
        {"step": 0},
        None,
        thread_id="t1",
        checkpoint_ns="",
        checkpoint_id="c1",
        key_provider=provider,
    )
    with pytest.raises(ValueError, match="no KeyProvider"):
        envelope.unpack_checkpoint(data, thread_id="t1")


def test_unpack_checkpoint_rejects_unencrypted_object_when_key_provider_configured():
    # A saver configured with encryption= must not silently accept an
    # object that was never encrypted (a bug, a race, or a stripped
    # envelope) -- "encrypted at rest" has to be enforced per object.
    data = envelope.pack_checkpoint({"v": 1, "id": "c1"}, {"step": 0}, None)
    with pytest.raises(ValueError, match="never encrypted"):
        envelope.unpack_checkpoint(
            data,
            thread_id="t1",
            checkpoint_ns="",
            checkpoint_id="c1",
            key_provider=_StaticKeyProvider(),
        )


def test_unpack_checkpoint_rejects_unknown_encryption_algorithm():
    bogus = ormsgpack.packb({"enc": "rot13", "key_id": "k1", "payload": b"x"})
    with pytest.raises(ValueError, match="rot13"):
        envelope.unpack_checkpoint(
            bogus, thread_id="t1", key_provider=_StaticKeyProvider()
        )


def test_resolve_key_provider_none_returns_none():
    assert envelope.resolve_key_provider(None) is None


def test_resolve_key_provider_without_extra_raises_import_error(monkeypatch):
    monkeypatch.setattr(envelope, "AESGCM", None)
    with pytest.raises(ImportError, match="encryption"):
        envelope.resolve_key_provider(_StaticKeyProvider())


def test_resolve_key_provider_rejects_object_without_get_key():
    class _NotAKeyProvider:
        pass

    with pytest.raises(TypeError, match="KeyProvider"):
        envelope.resolve_key_provider(_NotAKeyProvider())

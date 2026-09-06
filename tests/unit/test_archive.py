import io
import tarfile

import pytest

from langgraph_checkpoint_objectstorage import archive


def _raw_tar_with_member(name: str, data: bytes) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        info = tarfile.TarInfo(name=name)
        info.size = len(data)
        tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def test_pack_unpack_round_trip_preserves_paths_and_bytes():
    entries = {
        "t1/checkpoints/ckpt-1.msgpack": b"checkpoint bytes",
        "t1/writes/ckpt-1/task-1/0.msgpack": b"write bytes",
    }
    packed = archive.pack(entries)
    assert archive.unpack(packed) == entries


def test_pack_unpack_round_trip_preserves_binary_data_exactly():
    data = bytes(range(256)) * 4
    packed = archive.pack({"t1/checkpoints/ckpt-1.msgpack": data})
    assert archive.unpack(packed) == {"t1/checkpoints/ckpt-1.msgpack": data}


def test_unpack_empty_archive_returns_empty_dict():
    assert archive.unpack(archive.pack({})) == {}


def test_unpack_rejects_parent_directory_traversal():
    malicious = _raw_tar_with_member("../escape.txt", b"data")
    with pytest.raises(ValueError, match="unsafe path"):
        archive.unpack(malicious)


def test_unpack_rejects_absolute_path():
    malicious = _raw_tar_with_member("/etc/passwd", b"data")
    with pytest.raises(ValueError, match="unsafe path"):
        archive.unpack(malicious)


def test_unpack_rejects_current_directory_component():
    malicious = _raw_tar_with_member("t1/./checkpoints/ckpt-1.msgpack", b"data")
    with pytest.raises(ValueError, match="unsafe path"):
        archive.unpack(malicious)


def test_unpack_rejects_backslash_in_path():
    malicious = _raw_tar_with_member(
        "evil\\..\\..\\pwned/checkpoints/ckpt-1.msgpack", b"data"
    )
    with pytest.raises(ValueError, match="unsafe path"):
        archive.unpack(malicious)


def test_unpack_rejects_non_tar_bytes():
    with pytest.raises(ValueError, match="not a valid archive"):
        archive.unpack(b"not a tar file")

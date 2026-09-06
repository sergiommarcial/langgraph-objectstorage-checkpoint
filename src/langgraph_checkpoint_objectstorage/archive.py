from __future__ import annotations

import io
import tarfile

from langgraph_checkpoint_objectstorage import keys


def pack(entries: dict[str, bytes]) -> bytes:
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w") as tar:
        for path, data in entries.items():
            info = tarfile.TarInfo(name=path)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return buffer.getvalue()


def unpack(archive: bytes) -> dict[str, bytes]:
    entries: dict[str, bytes] = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(archive), mode="r") as tar:
            for member in tar.getmembers():
                if not member.isfile():
                    continue
                if not _is_safe_path(member.name):
                    raise ValueError(f"unsafe path in archive: {member.name!r}")
                extracted = tar.extractfile(member)
                entries[member.name] = extracted.read()
    except tarfile.TarError as exc:
        raise ValueError(f"not a valid archive: {exc}") from exc
    return entries


def _is_safe_path(path: str) -> bool:
    if path.startswith("/"):
        return False
    return all(keys.is_safe_segment(part) for part in path.split("/"))

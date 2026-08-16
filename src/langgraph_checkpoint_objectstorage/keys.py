from __future__ import annotations


def _thread_ns_root(root: str, thread_id: str, checkpoint_ns: str) -> str:
    parts = [root, thread_id]
    if checkpoint_ns:
        parts.append(checkpoint_ns)
    return "/".join(parts)


def checkpoints_prefix(root: str, thread_id: str, checkpoint_ns: str) -> str:
    return f"{_thread_ns_root(root, thread_id, checkpoint_ns)}/checkpoints/"


def checkpoint_key(
    root: str, thread_id: str, checkpoint_ns: str, checkpoint_id: str
) -> str:
    return (
        f"{checkpoints_prefix(root, thread_id, checkpoint_ns)}{checkpoint_id}.msgpack"
    )


def checkpoint_id_from_key(key: str) -> str:
    filename = key.rsplit("/", 1)[-1]
    if not filename.endswith(".msgpack"):
        raise ValueError(f"not a checkpoint key: {key!r}")
    return filename[: -len(".msgpack")]


def writes_prefix(
    root: str, thread_id: str, checkpoint_ns: str, checkpoint_id: str
) -> str:
    return f"{_thread_ns_root(root, thread_id, checkpoint_ns)}/writes/{checkpoint_id}/"


def write_key(
    root: str,
    thread_id: str,
    checkpoint_ns: str,
    checkpoint_id: str,
    task_id: str,
    idx: int,
) -> str:
    prefix = writes_prefix(root, thread_id, checkpoint_ns, checkpoint_id)
    return f"{prefix}{task_id}/{idx}.msgpack"


def thread_prefix(root: str, thread_id: str) -> str:
    return f"{root}/{thread_id}/"

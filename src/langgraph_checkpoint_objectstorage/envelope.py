from __future__ import annotations

from typing import Any

import ormsgpack
from langgraph.checkpoint.base import Checkpoint, CheckpointMetadata
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

_serde = JsonPlusSerializer()


def pack_checkpoint(
    checkpoint: Checkpoint,
    metadata: CheckpointMetadata,
    parent_checkpoint_id: str | None,
) -> bytes:
    ck_type, ck_bytes = _serde.dumps_typed(checkpoint)
    md_type, md_bytes = _serde.dumps_typed(metadata)
    return ormsgpack.packb(
        {
            "checkpoint": [ck_type, ck_bytes],
            "metadata": [md_type, md_bytes],
            "parent_checkpoint_id": parent_checkpoint_id,
        }
    )


def unpack_checkpoint(
    data: bytes,
) -> tuple[Checkpoint, CheckpointMetadata, str | None]:
    obj = ormsgpack.unpackb(data)
    checkpoint = _serde.loads_typed(tuple(obj["checkpoint"]))
    metadata = _serde.loads_typed(tuple(obj["metadata"]))
    return checkpoint, metadata, obj["parent_checkpoint_id"]


def pack_write(task_id: str, idx: int, channel: str, value: Any) -> bytes:
    v_type, v_bytes = _serde.dumps_typed(value)
    return ormsgpack.packb(
        {
            "task_id": task_id,
            "idx": idx,
            "channel": channel,
            "type": v_type,
            "value": v_bytes,
        }
    )


def unpack_write(data: bytes) -> tuple[str, int, str, Any]:
    obj = ormsgpack.unpackb(data)
    value = _serde.loads_typed((obj["type"], obj["value"]))
    return obj["task_id"], obj["idx"], obj["channel"], value

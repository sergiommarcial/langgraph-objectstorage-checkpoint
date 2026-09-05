from __future__ import annotations

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def seed_checkpoint_chain(
    saver: ObjectStorageSaver, thread_id: str, count: int, checkpoint_ns: str = ""
) -> list[str]:
    checkpoint_ids = [f"ckpt-{i:06d}" for i in range(count)]
    parent_id = None
    for step, checkpoint_id in enumerate(checkpoint_ids):
        config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": parent_id,
            }
        }
        checkpoint = {
            "v": 1,
            "id": checkpoint_id,
            "ts": "2026-01-01T00:00:00+00:00",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
        }
        saver.put(config, checkpoint, {"source": "input", "step": step}, {})
        parent_id = checkpoint_id
    return checkpoint_ids

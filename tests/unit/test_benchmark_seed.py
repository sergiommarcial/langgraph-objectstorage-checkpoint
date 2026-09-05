import fsspec

from tests.benchmark.seed import seed_checkpoint_chain
from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def _saver(tmp_path) -> ObjectStorageSaver:
    return ObjectStorageSaver(fsspec.filesystem("file"), str(tmp_path))


def test_seed_checkpoint_chain_returns_ids_oldest_first(tmp_path):
    saver = _saver(tmp_path)
    checkpoint_ids = seed_checkpoint_chain(saver, "t1", count=5)
    assert checkpoint_ids == sorted(checkpoint_ids)
    assert len(checkpoint_ids) == 5


def test_seed_checkpoint_chain_links_each_checkpoint_to_its_predecessor(tmp_path):
    saver = _saver(tmp_path)
    checkpoint_ids = seed_checkpoint_chain(saver, "t1", count=5)

    latest = saver.get_tuple({"configurable": {"thread_id": "t1", "checkpoint_ns": ""}})
    assert latest.checkpoint["id"] == checkpoint_ids[-1]
    assert latest.parent_config["configurable"]["checkpoint_id"] == checkpoint_ids[-2]

    oldest = saver.get_tuple(
        {
            "configurable": {
                "thread_id": "t1",
                "checkpoint_ns": "",
                "checkpoint_id": checkpoint_ids[0],
            }
        }
    )
    assert oldest.parent_config is None

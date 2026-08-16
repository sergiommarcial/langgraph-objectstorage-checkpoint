import tempfile

import fsspec
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


async def test_local_conformance():
    @checkpointer_test(name="ObjectStorageSaver-local")
    async def _local_checkpointer():
        with tempfile.TemporaryDirectory() as tmp:
            yield ObjectStorageSaver(fsspec.filesystem("file"), tmp)

    report = await validate(_local_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

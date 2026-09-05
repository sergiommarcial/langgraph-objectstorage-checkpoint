import tempfile

import fsspec
import pytest
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


@pytest.mark.parametrize("compression", ["none", "zlib", "lzma", "zstd"])
async def test_local_conformance(compression):
    @checkpointer_test(name=f"ObjectStorageSaver-local-{compression}")
    async def _local_checkpointer():
        with tempfile.TemporaryDirectory() as tmp:
            yield ObjectStorageSaver(
                fsspec.filesystem("file"), tmp, compression=compression
            )

    report = await validate(_local_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

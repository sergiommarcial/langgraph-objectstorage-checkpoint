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


class _StaticKeyProvider:
    def get_key(self, thread_id, key_id=None):
        return "k1", b"0" * 32


@pytest.mark.parametrize("compression", ["none", "zlib"])
async def test_local_conformance_with_encryption(compression):
    @checkpointer_test(name=f"ObjectStorageSaver-local-encrypted-{compression}")
    async def _local_checkpointer():
        with tempfile.TemporaryDirectory() as tmp:
            yield ObjectStorageSaver(
                fsspec.filesystem("file"),
                tmp,
                compression=compression,
                encryption=_StaticKeyProvider(),
            )

    report = await validate(_local_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

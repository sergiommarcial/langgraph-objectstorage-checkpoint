import os
import uuid

import fsspec
import pytest
from fsspec.asyn import AsyncFileSystem
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def test_gcs_protocol_resolves_to_gcsfs():
    fs_cls = fsspec.get_filesystem_class("gcs")
    assert fs_cls.__module__.startswith("gcsfs")
    assert issubclass(fs_cls, AsyncFileSystem)


@pytest.mark.skipif(
    not os.environ.get("GCS_TEST_BUCKET"),
    reason="set GCS_TEST_BUCKET (+ GCP credentials) to run real-bucket GCS conformance",
)
async def test_gcs_conformance():
    bucket = os.environ["GCS_TEST_BUCKET"]

    @checkpointer_test(name="ObjectStorageSaver-gcs")
    async def _gcs_checkpointer():
        prefix = f"{bucket}/{uuid.uuid4()}"
        yield ObjectStorageSaver.from_conn_string(f"gcs://{prefix}")

    report = await validate(_gcs_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

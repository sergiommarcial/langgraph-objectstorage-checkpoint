import uuid

from gcsfs.core import GCSFileSystem
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


async def test_gcs_conformance_against_fake_gcs_server(
    fake_gcs_endpoint, gcs_compose_bucket
):
    prefix = f"{gcs_compose_bucket}/{uuid.uuid4()}"

    @checkpointer_test(name="ObjectStorageSaver-gcs-compose")
    async def _gcs_checkpointer():
        # Deliberately gcsfs.core.GCSFileSystem, not from_conn_string's
        # fsspec-registry resolution (ExtendedGcsFileSystem) -- see the
        # comment on gcs_compose_bucket in conftest.py for why. This is a
        # test-scoped choice only: from_conn_string's real behavior for
        # library consumers is untouched.
        fs = GCSFileSystem(endpoint_url=fake_gcs_endpoint, token="anon")
        yield ObjectStorageSaver(fs, prefix)

    report = await validate(_gcs_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

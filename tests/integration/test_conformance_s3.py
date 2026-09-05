import uuid

import pytest
from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


@pytest.mark.parametrize("compression", ["none", "zstd"])
async def test_s3_conformance(moto_s3_endpoint, s3_bucket, compression):
    prefix = f"{s3_bucket}/{uuid.uuid4()}"

    @checkpointer_test(name=f"ObjectStorageSaver-s3-{compression}")
    async def _s3_checkpointer():
        yield ObjectStorageSaver.from_conn_string(
            f"s3://{prefix}",
            key="testing",
            secret="testing",
            client_kwargs={"endpoint_url": moto_s3_endpoint},
            compression=compression,
        )

    report = await validate(_s3_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()


class _StaticKeyProvider:
    def get_key(self, thread_id, key_id=None):
        return "k1", b"0" * 32


async def test_s3_conformance_with_encryption(moto_s3_endpoint, s3_bucket):
    prefix = f"{s3_bucket}/{uuid.uuid4()}"

    @checkpointer_test(name="ObjectStorageSaver-s3-encrypted")
    async def _s3_checkpointer():
        yield ObjectStorageSaver.from_conn_string(
            f"s3://{prefix}",
            key="testing",
            secret="testing",
            client_kwargs={"endpoint_url": moto_s3_endpoint},
            encryption=_StaticKeyProvider(),
        )

    report = await validate(_s3_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

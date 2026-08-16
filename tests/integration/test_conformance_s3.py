import uuid

from langgraph.checkpoint.conformance import checkpointer_test, validate
from langgraph.checkpoint.conformance.report import ProgressCallbacks

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


async def test_s3_conformance(moto_s3_endpoint, s3_bucket):
    prefix = f"{s3_bucket}/{uuid.uuid4()}"

    @checkpointer_test(name="ObjectStorageSaver-s3")
    async def _s3_checkpointer():
        yield ObjectStorageSaver.from_conn_string(
            f"s3://{prefix}",
            key="testing",
            secret="testing",
            client_kwargs={"endpoint_url": moto_s3_endpoint},
        )

    report = await validate(_s3_checkpointer, progress=ProgressCallbacks.default())
    report.print_report()
    assert report.passed_all_base()

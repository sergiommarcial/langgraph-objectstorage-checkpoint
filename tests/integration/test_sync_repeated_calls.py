"""Regression test for a real bug found while building a usage example:
repeated sync saver.put()/get_tuple() calls against S3/GCS broke with
"Event loop is closed". Two compounding causes:

1. asyncio.run() opens and tears down a fresh event loop per call, but
   s3fs/gcsfs bind their aiohttp session to whichever loop is running the
   first time they're used -- a second asyncio.run() call (a new loop)
   then breaks against that same session. Fixed by routing sync calls
   through the filesystem's own persistent background loop instead
   (ObjectStorageSaver._run_sync).
2. fsspec caches filesystem instances globally by constructor kwargs, so
   two savers built from the same URI/credentials (or the same saver
   touched from two different loop contexts, e.g. an async pytest test
   followed by a sync one) shared one session and could poison each
   other. Fixed with skip_instance_cache=True in from_conn_string.

Both are needed: the first makes repeated sync calls on one saver work,
the second stops that saver's filesystem from being silently shared with
anything else in the process.
"""

import uuid

from gcsfs.core import GCSFileSystem

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {},
        "channel_versions": {},
        "versions_seen": {},
    }


def _assert_survives_repeated_sync_calls(saver: ObjectStorageSaver) -> None:
    config = {"configurable": {"thread_id": "t1", "checkpoint_ns": ""}}

    for i in range(4):
        stored = saver.put(config, _checkpoint(f"ckpt-{i}"), {"step": i}, {})
        assert stored["configurable"]["checkpoint_id"] == f"ckpt-{i}"

    tup = saver.get_tuple(config)
    assert tup is not None
    assert tup.checkpoint["id"] == "ckpt-3"

    results = list(saver.list(config))
    assert [t.checkpoint["id"] for t in results] == [
        "ckpt-3",
        "ckpt-2",
        "ckpt-1",
        "ckpt-0",
    ]


def test_sync_put_survives_multiple_calls_against_s3(moto_s3_endpoint, s3_bucket):
    prefix = f"{s3_bucket}/{uuid.uuid4()}"
    saver = ObjectStorageSaver.from_conn_string(
        f"s3://{prefix}",
        key="testing",
        secret="testing",
        client_kwargs={"endpoint_url": moto_s3_endpoint},
    )
    _assert_survives_repeated_sync_calls(saver)


def test_sync_put_survives_multiple_calls_against_gcs(
    fake_gcs_endpoint, gcs_compose_bucket
):
    prefix = f"{gcs_compose_bucket}/{uuid.uuid4()}"
    # gcsfs.core.GCSFileSystem directly, not from_conn_string's
    # fsspec-registry resolution -- see the comment on gcs_compose_bucket
    # in conftest.py for why (ExtendedGcsFileSystem's HNS gRPC probe
    # doesn't work against fake-gcs-server).
    fs = GCSFileSystem(
        endpoint_url=fake_gcs_endpoint, token="anon", skip_instance_cache=True
    )
    saver = ObjectStorageSaver(fs, prefix)
    _assert_survives_repeated_sync_calls(saver)

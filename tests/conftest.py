import socket

import boto3
import pytest
from moto.server import ThreadedMotoServer


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def moto_s3_endpoint():
    port = _free_port()
    server = ThreadedMotoServer(port=port)
    server.start()
    yield f"http://127.0.0.1:{port}"
    server.stop()


@pytest.fixture
def s3_bucket(moto_s3_endpoint):
    client = boto3.client(
        "s3",
        endpoint_url=moto_s3_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    bucket = "test-checkpoints"
    client.create_bucket(Bucket=bucket)
    return bucket


def _skip_unless_reachable(host: str, port: int, service: str) -> str:
    try:
        with socket.create_connection((host, port), timeout=0.5):
            pass
    except OSError:
        pytest.skip(
            f"{service} not reachable at {host}:{port} -- "
            f"run `docker compose up -d` first"
        )
    return f"http://{host}:{port}"


@pytest.fixture(scope="session")
def moto_compose_endpoint():
    # 5001, not moto's default 5000 -- see the port comment in
    # docker-compose.yaml (macOS AirPlay Receiver squats on 5000).
    return _skip_unless_reachable("127.0.0.1", 5001, "moto-server (docker-compose)")


@pytest.fixture
def s3_compose_bucket(moto_compose_endpoint):
    client = boto3.client(
        "s3",
        endpoint_url=moto_compose_endpoint,
        aws_access_key_id="testing",
        aws_secret_access_key="testing",
        region_name="us-east-1",
    )
    bucket = "test-checkpoints"
    try:
        client.create_bucket(Bucket=bucket)
    except client.exceptions.BucketAlreadyOwnedByYou:
        pass
    except Exception as exc:
        pytest.skip(
            f"port 5001 is open but didn't behave like moto-server -- "
            f"is something else listening there? ({exc})"
        )
    return bucket


@pytest.fixture(scope="session")
def fake_gcs_endpoint():
    return _skip_unless_reachable("127.0.0.1", 4443, "fake-gcs-server (docker-compose)")


@pytest.fixture
def gcs_compose_bucket(fake_gcs_endpoint):
    # Deliberately a plain REST call, not gcsfs -- creating (and caching,
    # per fsspec's instance cache) a GCSFileSystem here and touching it
    # synchronously would bind its aiohttp session to fsspec's sync-bridge
    # background-thread loop. The test later awaits that same cached
    # instance's async-native methods directly on pytest-asyncio's own
    # loop, which fails with "attached to a different loop" since the
    # session belongs to a different loop than the one awaiting it.
    # A REST call here touches no gcsfs instance at all, so there's
    # nothing to mis-bind.
    import requests

    bucket = "test-checkpoints"
    resp = requests.post(
        f"{fake_gcs_endpoint}/storage/v1/b",
        params={"project": "test"},
        json={"name": bucket},
        timeout=5,
    )
    if resp.status_code not in (200, 409):
        pytest.skip(
            f"port 4443 is open but didn't behave like fake-gcs-server -- "
            f"is something else listening there? ({resp.status_code}: {resp.text[:200]})"
        )
    return bucket

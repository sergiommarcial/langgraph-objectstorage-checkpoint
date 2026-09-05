from __future__ import annotations

import uuid

import fsspec
import pytest
import s3fs

from langgraph_checkpoint_objectstorage import ObjectStorageSaver

# moto_s3_endpoint/s3_bucket come from tests/conftest.py's normal
# directory-conftest inheritance -- tests/benchmark is a subdirectory of
# tests/, so no import is needed to reuse the same in-process moto server.


class _StaticKeyProvider:
    def get_key(self, thread_id, key_id=None):
        return "k1", b"0" * 32


ENVELOPE_CONFIGS = {
    "plain": {"compression": "none", "encryption": None},
    "compression": {"compression": "zstd", "encryption": None},
    "encryption": {"compression": "none", "encryption": _StaticKeyProvider()},
}


@pytest.fixture(params=["local", "s3"])
def backend(request, tmp_path, moto_s3_endpoint, s3_bucket):
    if request.param == "local":
        fs = fsspec.filesystem("file")
        root = str(tmp_path)
    else:
        fs = s3fs.S3FileSystem(
            key="testing",
            secret="testing",
            client_kwargs={"endpoint_url": moto_s3_endpoint},
            skip_instance_cache=True,
        )
        root = f"{s3_bucket}/{uuid.uuid4()}"
    return request.param, fs, root


@pytest.fixture(params=list(ENVELOPE_CONFIGS))
def envelope_name(request):
    return request.param


@pytest.fixture
def saver(backend, envelope_name):
    _, fs, root = backend
    return ObjectStorageSaver(fs, root, **ENVELOPE_CONFIGS[envelope_name])


@pytest.fixture
def saver_plain(backend):
    _, fs, root = backend
    return ObjectStorageSaver(fs, root, **ENVELOPE_CONFIGS["plain"])

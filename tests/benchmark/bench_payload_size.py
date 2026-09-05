from __future__ import annotations

import pytest

PAYLOAD_SIZES = {"small": 100, "medium": 10_000, "large": 1_000_000}


def _checkpoint(checkpoint_id: str, nbytes: int) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {"state": "x" * nbytes},
        "channel_versions": {},
        "versions_seen": {},
    }


@pytest.fixture(params=list(PAYLOAD_SIZES))
def payload_size_name(request):
    return request.param


def test_put_by_payload_size(
    benchmark, saver, backend, envelope_name, payload_size_name
):
    benchmark.extra_info.update(
        operation="put",
        backend=backend[0],
        envelope=envelope_name,
        payload_size=payload_size_name,
    )
    nbytes = PAYLOAD_SIZES[payload_size_name]
    config = {"configurable": {"thread_id": "bench-put-size", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-000000", nbytes)
    benchmark(saver.put, config, checkpoint, {"source": "input", "step": 0}, {})


def test_get_tuple_by_payload_size(
    benchmark, saver, backend, envelope_name, payload_size_name
):
    benchmark.extra_info.update(
        operation="get_tuple",
        backend=backend[0],
        envelope=envelope_name,
        payload_size=payload_size_name,
    )
    nbytes = PAYLOAD_SIZES[payload_size_name]
    config = {"configurable": {"thread_id": "bench-get-size", "checkpoint_ns": ""}}
    saver.put(
        config, _checkpoint("ckpt-000000", nbytes), {"source": "input", "step": 0}, {}
    )
    benchmark(saver.get_tuple, config)

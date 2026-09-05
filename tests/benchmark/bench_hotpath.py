from __future__ import annotations

_PAYLOAD = "x" * 10_000  # ~10KB, representative "typical" checkpoint state


def _checkpoint(checkpoint_id: str) -> dict:
    return {
        "v": 1,
        "id": checkpoint_id,
        "ts": "2026-01-01T00:00:00+00:00",
        "channel_values": {"state": _PAYLOAD},
        "channel_versions": {},
        "versions_seen": {},
    }


def test_put(benchmark, saver, backend, envelope_name):
    benchmark.extra_info.update(
        operation="put", backend=backend[0], envelope=envelope_name
    )
    config = {"configurable": {"thread_id": "bench-put", "checkpoint_ns": ""}}
    checkpoint = _checkpoint("ckpt-000000")
    benchmark(saver.put, config, checkpoint, {"source": "input", "step": 0}, {})


def test_get_tuple(benchmark, saver, backend, envelope_name):
    benchmark.extra_info.update(
        operation="get_tuple", backend=backend[0], envelope=envelope_name
    )
    config = {"configurable": {"thread_id": "bench-get", "checkpoint_ns": ""}}
    saver.put(config, _checkpoint("ckpt-000000"), {"source": "input", "step": 0}, {})
    benchmark(saver.get_tuple, config)


def test_put_writes(benchmark, saver, backend, envelope_name):
    benchmark.extra_info.update(
        operation="put_writes", backend=backend[0], envelope=envelope_name
    )
    thread_id = "bench-writes"
    checkpoint_id = "ckpt-000000"
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    saver.put(config, _checkpoint(checkpoint_id), {"source": "input", "step": 0}, {})
    write_config = {
        "configurable": {
            "thread_id": thread_id,
            "checkpoint_ns": "",
            "checkpoint_id": checkpoint_id,
        }
    }
    # "__error__" is one of the control channels (WRITES_IDX_MAP) that
    # always overwrites rather than first-write-wins, so repeated benchmark
    # rounds measure a real write each time instead of hitting the
    # already-exists skip path after round 1.
    benchmark(saver.put_writes, write_config, [("__error__", _PAYLOAD)], "bench-task")

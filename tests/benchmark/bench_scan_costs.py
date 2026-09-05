from __future__ import annotations

import uuid

import pytest

from tests.benchmark.seed import seed_checkpoint_chain

HISTORY_SIZES = [10, 100, 1000]


@pytest.fixture(params=HISTORY_SIZES)
def seeded_thread(request, saver_plain):
    count = request.param
    thread_id = f"bench-scan-{uuid.uuid4()}"
    checkpoint_ids = seed_checkpoint_chain(saver_plain, thread_id, count)
    return count, thread_id, checkpoint_ids


def test_get_tuple_latest_by_history_size(
    benchmark, saver_plain, backend, seeded_thread
):
    count, thread_id, _ = seeded_thread
    benchmark.extra_info.update(
        operation="get_tuple_latest", backend=backend[0], history_size=count
    )
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    benchmark(saver_plain.get_tuple, config)


def test_list_filter_by_history_size(benchmark, saver_plain, backend, seeded_thread):
    count, thread_id, checkpoint_ids = seeded_thread
    benchmark.extra_info.update(
        operation="list_filter", backend=backend[0], history_size=count
    )
    config = {"configurable": {"thread_id": thread_id, "checkpoint_ns": ""}}
    midpoint_step = len(checkpoint_ids) // 2

    def _consume():
        return list(saver_plain.list(config, filter={"step": midpoint_step}))

    benchmark(_consume)

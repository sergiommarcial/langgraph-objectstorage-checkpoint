from __future__ import annotations

import uuid

import pytest

from tests.benchmark.seed import seed_checkpoint_chain

HISTORY_SIZES = [10, 100, 1000]


@pytest.mark.parametrize("history_size", HISTORY_SIZES)
def test_delete_thread_by_history_size(benchmark, saver_plain, backend, history_size):
    benchmark.extra_info.update(
        operation="delete_thread", backend=backend[0], history_size=history_size
    )

    def _setup():
        thread_id = f"bench-delete-{uuid.uuid4()}"
        seed_checkpoint_chain(saver_plain, thread_id, history_size)
        return (thread_id,), {}

    benchmark.pedantic(saver_plain.delete_thread, setup=_setup, rounds=5, iterations=1)

"""Multiple sessions (thread_ids) against GCS, run concurrently via the
async API.

See examples/poetry/gcs for the sequential/resume version. They're
separate examples, not two code paths in one, because mixing sync and
async calls on the *same* ObjectStorageSaver instance against an
async-native backend (S3/GCS) breaks: sync calls run on fsspec's
persistent background-thread loop, async calls run on whichever loop the
caller provides, and the underlying aiohttp session can only belong to
one loop at a time. Each example builds its own saver instance, used
consistently as sync-only or async-only.
"""

import asyncio
import operator
import os
import uuid
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


class SessionState(TypedDict):
    turns: Annotated[list[str], operator.add]


def respond(state: SessionState) -> SessionState:
    turn_number = len(state["turns"]) + 1
    return {"turns": [f"turn {turn_number}"]}


builder = StateGraph(SessionState)
builder.add_node("respond", respond)
builder.add_edge(START, "respond")
builder.add_edge("respond", END)


def make_saver() -> ObjectStorageSaver:
    # A fresh prefix each run so the printed output is the same shape
    # every time -- see the matching comment in examples/poetry/gcs/main.py.
    run_prefix = uuid.uuid4().hex[:8]
    print(f"run prefix: {run_prefix}")

    gcs_emulator = os.environ.get("STORAGE_EMULATOR_HOST")
    if gcs_emulator:
        # See the matching comment in examples/poetry/gcs/main.py -- the
        # base gcsfs.core.GCSFileSystem sidesteps ExtendedGcsFileSystem's
        # gRPC HNS probe, which doesn't work against a plain-HTTP emulator.
        from gcsfs.core import GCSFileSystem

        fs = GCSFileSystem(endpoint_url=gcs_emulator, token="anon")
        return ObjectStorageSaver(
            fs, f"test-checkpoints/poetry-gcs-async-example/{run_prefix}"
        )
    return ObjectStorageSaver.from_conn_string(
        f"gcs://your-bucket/checkpoints/{run_prefix}"
    )


async def run_one(graph, session_id: str) -> tuple[str, list[str]]:
    config = {"configurable": {"thread_id": session_id}}
    for _ in range(2):
        result = await graph.ainvoke({"turns": []}, config)
    return session_id, result["turns"]


async def run_concurrent_sessions(graph, session_ids: list[str]) -> None:
    print("--- concurrent sessions ---")
    results = await asyncio.gather(*(run_one(graph, sid) for sid in session_ids))
    for session_id, turns in results:
        print(f"{session_id}: {turns}")


async def main() -> None:
    saver = make_saver()
    graph = builder.compile(checkpointer=saver)

    session_ids = ["session-x", "session-y", "session-z"]
    await run_concurrent_sessions(graph, session_ids)


if __name__ == "__main__":
    asyncio.run(main())

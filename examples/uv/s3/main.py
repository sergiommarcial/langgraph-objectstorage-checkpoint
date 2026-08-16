"""Multiple independent sessions (thread_ids) against S3, run sequentially,
then one is resumed later from where it left off.

See examples/uv/s3-async for the concurrent/async version -- it's a
separate example, not a second code path in this one, because mixing sync
and async calls on the *same* ObjectStorageSaver instance against an
async-native backend (S3/GCS) breaks: sync calls run on fsspec's
persistent background-thread loop, async calls run on whichever loop the
caller provides, and the underlying aiohttp session can only belong to
one loop at a time. Each example builds its own saver instance, used
consistently as sync-only or async-only.
"""

import operator
import os
import uuid
from typing import Annotated, TypedDict

from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


class SessionState(TypedDict):
    # operator.add as the reducer means each invoke() call's returned
    # "turns" list is appended to what the checkpointer already has for
    # this thread_id, not overwritten -- that's what makes calling
    # invoke() again later on the same thread_id a "resume", not a reset.
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
    # every time -- checkpoints genuinely do persist across process
    # restarts (that's the point of this library), but a demo re-reading
    # old sessions on every run just makes the output confusing.
    run_prefix = uuid.uuid4().hex[:8]
    print(f"run prefix: {run_prefix}")

    # AWS_ENDPOINT_URL is a standard boto3-recognized env var -- set it to
    # point at a local moto-server (`docker compose up -d` from the repo
    # root) instead of real S3, no AWS account needed. Unlike GCS, S3 via
    # from_conn_string's default resolution has no emulator-specific quirk
    # to work around, so both branches just use from_conn_string directly.
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint_url:
        return ObjectStorageSaver.from_conn_string(
            f"s3://test-checkpoints/uv-s3-example/{run_prefix}",
            key="testing",
            secret="testing",
            client_kwargs={"endpoint_url": endpoint_url},
        )
    return ObjectStorageSaver.from_conn_string(
        f"s3://your-bucket/checkpoints/{run_prefix}"
    )


def run_sequential_sessions(graph, session_ids: list[str]) -> None:
    print("--- sequential sessions ---")
    for session_id in session_ids:
        config = {"configurable": {"thread_id": session_id}}
        for _ in range(2):
            result = graph.invoke({"turns": []}, config)
        print(f"{session_id}: {result['turns']}")


def resume_a_session(graph, session_id: str) -> None:
    print("--- resuming an earlier session ---")
    config = {"configurable": {"thread_id": session_id}}
    # No checkpoint_id given: the saver resolves the latest checkpoint for
    # this thread_id automatically, so this call continues from wherever
    # run_sequential_sessions left off rather than starting over.
    result = graph.invoke({"turns": []}, config)
    print(f"{session_id} resumed: {result['turns']}")

    history = list(graph.get_state_history(config))
    print(f"{session_id} has {len(history)} checkpoint(s) stored")


def main() -> None:
    saver = make_saver()
    graph = builder.compile(checkpointer=saver)

    session_ids = ["session-a", "session-b", "session-c"]
    run_sequential_sessions(graph, session_ids)
    resume_a_session(graph, session_ids[0])


if __name__ == "__main__":
    main()

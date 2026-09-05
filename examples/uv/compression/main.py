"""Same checkpoint, written once uncompressed and once with `zstd`
compression, to show the tradeoff `compression` makes explicit: smaller
objects on disk in exchange for CPU spent compressing/decompressing.
"""

import os

from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


def append_history(state: dict) -> dict:
    # A repetitive, compressible payload -- the kind of large channel
    # value (accumulated message history, tool output) that makes
    # compression worth turning on in the first place.
    return {"history": state["history"] + "message " * 2000}


builder = StateGraph(dict)
builder.add_node("append_history", append_history)
builder.add_edge(START, "append_history")
builder.add_edge("append_history", END)

config = {"configurable": {"thread_id": "1"}}

# `compression="none"` (the default) -- byte-identical to every release
# before this option existed.
plain_saver = ObjectStorageSaver.from_conn_string("file://./checkpoints/none")
plain_graph = builder.compile(checkpointer=plain_saver)
plain_graph.invoke({"history": ""}, config)

# `compression="zstd"` needs the `compression` extra
# (`pip install "langgraph-checkpoint-objectstorage[compression]"`).
# `zlib`/`lzma` work the same way with no extra dependency. Setting it as
# a `?compression=...` query parameter here is equivalent to passing
# `compression="zstd"` directly to `from_conn_string` -- just an
# alternative spelling for the same option.
compressed_saver = ObjectStorageSaver.from_conn_string(
    "file://./checkpoints/zstd?compression=zstd"
)
compressed_graph = builder.compile(checkpointer=compressed_saver)
compressed_graph.invoke({"history": ""}, config)


def largest_checkpoint_size(root: str) -> int:
    checkpoints_dir = os.path.join(root, "1", "checkpoints")
    return max(
        os.path.getsize(os.path.join(checkpoints_dir, name))
        for name in os.listdir(checkpoints_dir)
    )


plain_size = largest_checkpoint_size("./checkpoints/none")
compressed_size = largest_checkpoint_size("./checkpoints/zstd")

print(f"compression=none: {plain_size:,} bytes")
print(f"compression=zstd: {compressed_size:,} bytes")
print(f"reduction: {100 * (1 - compressed_size / plain_size):.0f}%")

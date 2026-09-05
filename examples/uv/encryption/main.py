"""Same checkpoint, written once without encryption and once with
`encryption` set, to show what `encryption` actually buys you: the raw
object on disk goes from readable plaintext to opaque ciphertext.
"""

import os

from langgraph.graph import END, START, StateGraph

from langgraph_checkpoint_objectstorage import ObjectStorageSaver


class StaticKeyProvider:
    """Minimal KeyProvider: one static key, no rotation.

    A real implementation would call out to a KMS or Vault instead of
    returning a hardcoded key -- see the README's Encryption section.
    """

    def __init__(self, key: bytes) -> None:
        self._key = key

    def get_key(self, thread_id: str, key_id: str | None = None) -> tuple[str, bytes]:
        return "k1", self._key


def record_note(state: dict) -> dict:
    # A sensitive value that should never be readable straight off disk --
    # the kind of thing `encryption` exists to protect.
    return {"note": "customer SSN on file: 123-45-6789"}


builder = StateGraph(dict)
builder.add_node("record_note", record_note)
builder.add_edge(START, "record_note")
builder.add_edge("record_note", END)

config = {"configurable": {"thread_id": "1"}}

# `encryption=None` (the default) -- byte-identical to every release
# before this option existed.
plain_saver = ObjectStorageSaver.from_conn_string("file://./checkpoints/plain")
plain_graph = builder.compile(checkpointer=plain_saver)
plain_graph.invoke({"note": ""}, config)

# `encryption=<KeyProvider>` needs the `encryption` extra
# (`pip install "langgraph-checkpoint-objectstorage[encryption]"`).
encrypted_saver = ObjectStorageSaver.from_conn_string(
    "file://./checkpoints/encrypted",
    encryption=StaticKeyProvider(key=b"0" * 32),
)
encrypted_graph = builder.compile(checkpointer=encrypted_saver)
encrypted_graph.invoke({"note": ""}, config)


def checkpoint_bytes(root: str) -> bytes:
    # Checkpoint filenames are time-sortable UUID6s -- the same "latest is
    # the lexicographic max" property ObjectStorageSaver itself relies on
    # for get_tuple(latest). A run produces more than one checkpoint (one
    # per superstep), so this is the one where `record_note` has run.
    checkpoints_dir = os.path.join(root, "1", "checkpoints")
    name = max(os.listdir(checkpoints_dir))
    with open(os.path.join(checkpoints_dir, name), "rb") as f:
        return f.read()


plain_bytes = checkpoint_bytes("./checkpoints/plain")
encrypted_bytes = checkpoint_bytes("./checkpoints/encrypted")
ssn = b"123-45-6789"

print(f"plain object has the SSN in the clear: {ssn in plain_bytes}")
print(f"encrypted object has the SSN in the clear: {ssn in encrypted_bytes}")

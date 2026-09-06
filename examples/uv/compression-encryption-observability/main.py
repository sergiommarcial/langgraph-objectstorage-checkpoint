"""One saver configured with `compression`, `encryption`, and `on_io`
together, to show the three options compose cleanly: compression and
encryption both apply to the same object, and `on_io` -- here wired
through `otel_on_io_adapter` to a real Prometheus scrape endpoint --
reports the real (compressed, encrypted) bytes written, not the original
payload size.

Prometheus is pull-based and metrics-only (no span ingestion -- see the
README's Observability section), so this exposes a `/metrics` endpoint
for `docker compose`'s Prometheus container to scrape rather than pushing
anything itself. The tracer below still creates spans (satisfying
`otel_on_io_adapter`'s required argument), it just has no exporter
attached, so they're created and discarded rather than sent anywhere.
"""

import os
import time

from langgraph.graph import END, START, StateGraph
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.trace import TracerProvider
from prometheus_client import start_http_server

from langgraph_checkpoint_objectstorage import ObjectStorageSaver, otel_on_io_adapter


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
    # A repetitive, compressible payload (like the compression example)
    # holding a sensitive value (like the encryption example) -- the kind
    # of channel that makes both options worth turning on together.
    return {"note": "customer SSN on file: 123-45-6789. " + "history " * 2000}


builder = StateGraph(dict)
builder.add_node("record_note", record_note)
builder.add_edge(START, "record_note")
builder.add_edge("record_note", END)

config = {"configurable": {"thread_id": "1"}}

start_http_server(9464)
tracer = TracerProvider().get_tracer("objectstorage-example")
meter = MeterProvider(metric_readers=[PrometheusMetricReader()]).get_meter(
    "objectstorage-example"
)

# `compression="zstd"` needs the `compression` extra, `encryption=` needs
# the `encryption` extra, `otel_on_io_adapter` needs the `observability`
# extra.
saver = ObjectStorageSaver.from_conn_string(
    "file://./checkpoints?compression=zstd",
    encryption=StaticKeyProvider(key=b"0" * 32),
    on_io=otel_on_io_adapter(tracer, meter=meter),
)
graph = builder.compile(checkpointer=saver)
graph.invoke({"note": ""}, config)


def checkpoint_bytes(root: str) -> bytes:
    # Checkpoint filenames are time-sortable UUID6s -- the same "latest is
    # the lexicographic max" property ObjectStorageSaver itself relies on
    # for get_tuple(latest). A run produces more than one checkpoint (one
    # per superstep), so this is the one where `record_note` has run.
    checkpoints_dir = os.path.join(root, "1", "checkpoints")
    name = max(os.listdir(checkpoints_dir))
    with open(os.path.join(checkpoints_dir, name), "rb") as f:
        return f.read()


raw = checkpoint_bytes("./checkpoints")
ssn = b"123-45-6789"
print(f"object on disk: {len(raw):,} bytes (payload was ~16KB uncompressed)")
print(f"has the SSN in the clear: {ssn in raw}")

print(
    "\nServing metrics at http://localhost:9464/metrics -- run "
    "`docker compose up -d` in this directory to scrape them into "
    "Prometheus, then query `objectstorage_io_duration_milliseconds_count` "
    "at http://localhost:9090. Ctrl+C to stop."
)
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    pass

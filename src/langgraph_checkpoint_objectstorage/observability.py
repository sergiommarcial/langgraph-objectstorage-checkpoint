from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

try:
    from opentelemetry import trace
    from opentelemetry.trace import Status, StatusCode
except ImportError:
    trace = None
    Status = None
    StatusCode = None


@dataclass(frozen=True)
class IOEvent:
    op: Literal["find", "cat", "pipe", "exists", "rm"]
    key: str
    count: int | None
    nbytes: int | None
    duration_ms: float
    error: BaseException | None


def otel_on_io_adapter(
    tracer: "trace.Tracer", meter: Any | None = None
) -> Callable[[IOEvent], None]:
    """Turn `IOEvent`s into OpenTelemetry spans, and optionally metrics.

    Requires the `observability` extra
    (`pip install "langgraph-checkpoint-objectstorage[observability]"`).
    Getting the resulting spans/metrics to a specific backend (Datadog,
    Prometheus, Dynatrace, ...) is exporter configuration on the caller's
    side -- see the README's Observability section for examples.
    """
    if trace is None:
        raise ImportError(
            "otel_on_io_adapter requires the 'observability' extra: "
            "pip install 'langgraph-checkpoint-objectstorage[observability]'"
        )
    duration_histogram = None
    bytes_counter = None
    count_histogram = None
    if meter is not None:
        duration_histogram = meter.create_histogram(
            "objectstorage.io.duration",
            unit="ms",
            description="I/O call duration by op",
        )
        bytes_counter = meter.create_counter(
            "objectstorage.io.bytes",
            unit="By",
            description="Bytes transferred by cat/pipe",
        )
        count_histogram = meter.create_histogram(
            "objectstorage.io.count",
            unit="1",
            description="Keys scanned by find",
        )

    def _on_io(event: IOEvent) -> None:
        # FileNotFoundError is normal control flow for this saver (an
        # empty thread's first get_tuple/list, deleting an already-gone
        # thread) -- not a real failure, so it's recorded on the span for
        # context but never flips status to ERROR or the metric's `error`
        # attribute, which would otherwise drown real failures in noise.
        is_real_error = event.error is not None and not isinstance(
            event.error, FileNotFoundError
        )

        end_ns = time.time_ns()
        start_ns = end_ns - int(event.duration_ms * 1_000_000)
        span = tracer.start_span(f"objectstorage.{event.op}", start_time=start_ns)
        span.set_attribute("objectstorage.key", event.key)
        if event.count is not None:
            span.set_attribute("objectstorage.count", event.count)
        if event.nbytes is not None:
            span.set_attribute("objectstorage.nbytes", event.nbytes)
        if event.error is not None:
            span.record_exception(event.error)
            if is_real_error:
                span.set_status(Status(StatusCode.ERROR, str(event.error)))
        span.end(end_time=end_ns)

        attrs = {"op": event.op, "error": is_real_error}
        if duration_histogram is not None:
            duration_histogram.record(event.duration_ms, attrs)
        if bytes_counter is not None and event.nbytes is not None:
            bytes_counter.add(event.nbytes, attrs)
        if count_histogram is not None and event.count is not None:
            count_histogram.record(event.count, attrs)

    return _on_io

import pytest
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import StatusCode

from langgraph_checkpoint_objectstorage import observability
from langgraph_checkpoint_objectstorage.observability import (
    IOEvent,
    otel_on_io_adapter,
)


def _tracer_with_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider.get_tracer("test"), exporter


def _meter_with_reader():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    return provider.get_meter("test"), reader


def test_otel_on_io_adapter_creates_span_with_event_attributes():
    tracer, exporter = _tracer_with_exporter()
    on_io = otel_on_io_adapter(tracer)
    event = IOEvent(
        op="find",
        key="root/thread-1",
        count=3,
        nbytes=None,
        duration_ms=12.5,
        error=None,
    )

    on_io(event)

    (span,) = exporter.get_finished_spans()
    assert span.name == "objectstorage.find"
    assert span.attributes["objectstorage.key"] == "root/thread-1"
    assert span.attributes["objectstorage.count"] == 3
    assert "objectstorage.nbytes" not in span.attributes
    assert span.status.status_code == StatusCode.UNSET


def test_otel_on_io_adapter_span_reflects_recorded_duration():
    tracer, exporter = _tracer_with_exporter()
    on_io = otel_on_io_adapter(tracer)
    event = IOEvent(
        op="cat", key="root/t/c1", count=None, nbytes=100, duration_ms=250.0, error=None
    )

    on_io(event)

    (span,) = exporter.get_finished_spans()
    assert (span.end_time - span.start_time) == pytest.approx(250_000_000, rel=0.05)


def test_otel_on_io_adapter_records_exception_on_error_event():
    tracer, exporter = _tracer_with_exporter()
    on_io = otel_on_io_adapter(tracer)
    error = OSError("disk full")
    event = IOEvent(
        op="pipe",
        key="root/t/c1",
        count=None,
        nbytes=None,
        duration_ms=1.0,
        error=error,
    )

    on_io(event)

    (span,) = exporter.get_finished_spans()
    assert span.status.status_code == StatusCode.ERROR
    (recorded_exc,) = span.events
    assert recorded_exc.name == "exception"


def test_otel_on_io_adapter_does_not_mark_file_not_found_as_error_status():
    # FileNotFoundError is normal control flow for this saver -- an empty
    # thread's first get_tuple/list, or deleting an already-gone thread --
    # not a real failure worth alerting on.
    tracer, exporter = _tracer_with_exporter()
    on_io = otel_on_io_adapter(tracer)
    error = FileNotFoundError("root/t/missing")
    event = IOEvent(
        op="cat",
        key="root/t/missing",
        count=None,
        nbytes=None,
        duration_ms=1.0,
        error=error,
    )

    on_io(event)

    (span,) = exporter.get_finished_spans()
    assert span.status.status_code == StatusCode.UNSET
    (recorded_exc,) = span.events
    assert recorded_exc.name == "exception"


def test_otel_on_io_adapter_metric_error_attr_false_for_file_not_found():
    tracer, _ = _tracer_with_exporter()
    meter, reader = _meter_with_reader()
    on_io = otel_on_io_adapter(tracer, meter=meter)
    event = IOEvent(
        op="cat",
        key="root/t/missing",
        count=None,
        nbytes=None,
        duration_ms=1.0,
        error=FileNotFoundError("root/t/missing"),
    )

    on_io(event)

    metrics_data = reader.get_metrics_data()
    points = [
        point
        for rm in metrics_data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == "objectstorage.io.duration"
        for point in metric.data.data_points
    ]
    (point,) = points
    assert point.attributes["error"] is False


def test_otel_on_io_adapter_without_meter_records_no_metrics():
    tracer, _ = _tracer_with_exporter()
    on_io = otel_on_io_adapter(tracer)
    event = IOEvent(
        op="cat", key="root/t/c1", count=None, nbytes=100, duration_ms=5.0, error=None
    )

    on_io(event)  # must not raise with meter=None


def test_otel_on_io_adapter_with_meter_records_duration_histogram_and_bytes_counter():
    tracer, _ = _tracer_with_exporter()
    meter, reader = _meter_with_reader()
    on_io = otel_on_io_adapter(tracer, meter=meter)
    event = IOEvent(
        op="pipe", key="root/t/c1", count=None, nbytes=256, duration_ms=42.0, error=None
    )

    on_io(event)

    metrics_data = reader.get_metrics_data()
    metric_names = {
        metric.name
        for rm in metrics_data.resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
    }
    assert "objectstorage.io.duration" in metric_names
    assert "objectstorage.io.bytes" in metric_names


def test_otel_on_io_adapter_with_meter_records_count_histogram_for_find():
    tracer, _ = _tracer_with_exporter()
    meter, reader = _meter_with_reader()
    on_io = otel_on_io_adapter(tracer, meter=meter)
    event = IOEvent(
        op="find", key="root/t1", count=1200, nbytes=None, duration_ms=8.0, error=None
    )

    on_io(event)

    points = [
        point
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
        if metric.name == "objectstorage.io.count"
        for point in metric.data.data_points
    ]
    (point,) = points
    assert point.sum == 1200
    assert point.attributes["op"] == "find"


def test_otel_on_io_adapter_with_meter_records_no_count_when_absent():
    tracer, _ = _tracer_with_exporter()
    meter, reader = _meter_with_reader()
    on_io = otel_on_io_adapter(tracer, meter=meter)
    event = IOEvent(
        op="cat", key="root/t/c1", count=None, nbytes=100, duration_ms=5.0, error=None
    )

    on_io(event)

    metric_names = {
        metric.name
        for rm in reader.get_metrics_data().resource_metrics
        for sm in rm.scope_metrics
        for metric in sm.metrics
    }
    assert "objectstorage.io.count" not in metric_names


def test_otel_on_io_adapter_raises_import_error_without_extra(monkeypatch):
    monkeypatch.setattr(observability, "trace", None)
    with pytest.raises(ImportError, match="observability"):
        otel_on_io_adapter(tracer=object())

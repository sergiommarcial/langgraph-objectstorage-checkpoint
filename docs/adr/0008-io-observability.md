# ADR 0008: Injectable I/O observability hook with an optional OpenTelemetry adapter

- **Status:** Accepted (implemented)
- **Date:** 2026-09-04
- **Owners:** ObjectStorageSaver maintainers

## Context

The README's Known limitations section already documents real costs:
`list(filter=...)`'s O(n) client-side scan, and `get_tuple(latest)`'s
full prefix listing on every resume. What it doesn't give a consumer is
any way to actually see those costs happening in their own deployment,
short of external network-level tracing they'd have to set up themselves.

`saver.py` already funnels every backend operation through one small
bridge (`_cat`/`_pipe`/`_find`/`_find_detailed`/`_exists`/`_rm`,
`saver.py:144-208`), specifically so backend-specific behavior has one
place to live. That same choke point is the natural place to observe
cost, without touching any of the business-logic methods that call into
it.

## Decision

Add `on_io: Callable[[IOEvent], None] | None = None` to
`ObjectStorageSaver.__init__` and `from_conn_string`. `IOEvent` is a
frozen dataclass:

```python
@dataclass(frozen=True)
class IOEvent:
    op: Literal["find", "cat", "pipe", "exists", "rm"]
    key: str            # the key or prefix involved
    count: int | None   # number of keys, for find
    nbytes: int | None  # payload size, for cat/pipe
    duration_ms: float
    error: BaseException | None
```

Each bridge method wraps its existing body with timing and calls
`on_io` once on completion, whether it succeeded or raised, if a
callback is configured. With no callback configured (the default), the
only added cost is a `None` check.

The bridge actually has six methods, not five --
`_find_detailed` (used by `delete_expired`'s full-bucket scan) is a
`find` call with `detail=True`, not a distinct operation, so it reports
as `op="find"` rather than adding a sixth `IOEvent.op` value. An
exception raised by `on_io` itself (as opposed to one from the I/O call
it's observing) is logged at debug level and never propagates -- a
broken observability callback must never break real I/O.

All six methods share one `_timed_io` async context manager (`saver.py`)
rather than repeating the same start/except/finally shape six times --
each method's body only fills in `count`/`nbytes` on a small mutable
`_IOTiming` object as it learns them. `_timed_io` catches
`BaseException`, not `Exception`: a cancelled task's
`asyncio.CancelledError` (which subclasses `BaseException`, not
`Exception`) must still reach `on_io` as the call's `error`, not look
like a clean success, since the call never actually completed. It's
always re-raised afterward, so this never changes what the caller sees
-- only what `on_io` is told. With no callback configured, `_timed_io`
returns immediately, before touching `time.monotonic()` at all, so the
"adds no overhead" claim above holds literally, not just approximately.

### No dependency by default, one adapter shipped alongside it

The callback itself adds nothing to core dependencies. A new
`observability.py` module ships one ready-made adapter,
`otel_on_io_adapter(tracer) -> Callable[[IOEvent], None]`, which turns
each `IOEvent` into an OpenTelemetry span with the event's fields as span
attributes. Importing `observability.py` requires `opentelemetry-api`,
gated behind a new `observability` extra; a consumer who isn't using
OTel never imports that module and never installs the dependency. A
consumer who is on OTel gets working spans in one line:

```python
ObjectStorageSaver(fs, root, on_io=otel_on_io_adapter(tracer))
```

instead of writing an OTel-specific callback from scratch. This keeps
one mechanism (the callback) rather than two parallel observability
systems: OTel support is just a callback implementation the library
happens to provide, not a separate code path through `saver.py`.

### How it fits the existing architecture

`saver.py`'s business-logic methods
(`_get_tuple`/`_put`/`_list`/`_put_writes`/`_delete_thread`) are
untouched. They already call into the `_cat`/`_pipe`/`_find`/`_exists`/
`_rm` bridge rather than talking to `fs` directly, so timing lives
entirely in that bridge, in one place.

### What changes for someone who opts in

- Real visibility into the O(n) costs the README already documents,
  instead of hitting them blind in production.
- Near-zero overhead when no callback is configured.
- The callback runs synchronously, on the same coroutine or thread as
  the I/O call it's observing. A slow or blocking `on_io` implementation
  adds real latency to every operation it wraps. A consumer who wants to
  ship events somewhere remote should hand off to a queue themselves,
  rather than doing that I/O inside the callback.

## Alternatives considered

### A. Native OpenTelemetry spans built directly into `saver.py`

Have the library create OTel spans itself around every bridge call,
without a separate callback abstraction.

Rejected: it forces the `opentelemetry-api` dependency onto every
consumer regardless of whether they use OTel, and gives a consumer who
wants statsd, plain logging, or anything else no path except also
carrying that dependency. A callback with an optional adapter serves
both cases from one mechanism instead of picking OTel specifically.

### B. Return timing/count data from each public method instead of a callback

Have `get_tuple`/`list`/`put`/`put_writes` return or attach timing
information directly, instead of a side-channel callback.

Rejected: it changes the return shape of methods that are public
`BaseCheckpointSaver` contract surface, governed by the conformance
suite. A callback is purely additive. It never touches those signatures,
so `report.passed_all_base()` doesn't need to account for it at all.

### C. Do nothing (status quo)

Rejected: it leaves the documented O(n) costs genuinely invisible until a
consumer hits them in production, which is exactly the failure mode
[ADR 0002](0002-slatedb.md)'s SlateDB proposal exists to eventually fix
at the root. A callback makes today's cost observable in the meantime,
independent of, and not blocked by, that larger ADR.

## Consequences

**Positive**

- Real visibility into documented costs, with zero forced dependency.
- One hook point, entirely outside `saver.py`'s business logic.
- Works today, independent of whether ADR 0002 ever ships.

**Negative / risks**

- The callback runs synchronously in the hot path; a badly behaved
  consumer callback becomes a self-inflicted latency problem the library
  can't protect against.
- `IOEvent`'s shape becomes public surface once consumers depend on it,
  and needs to stay stable going forward.
- The OTel adapter is one more small module to maintain, even though
  it's entirely opt-in.

## Applicability

**Worth enabling for:**

- Production deployments that want visibility into the documented O(n)
  paths (`list(filter=...)`, `get_tuple(latest)`).
- Debugging unexpected latency in an existing deployment.

**Skip for:**

- Simple or local deployments where the underlying cost is already
  negligible.
- Anyone not ready to keep an `on_io` callback fast, since a slow one
  adds latency to every call it wraps.

## Follow-up / open questions

- Resolved: `on_io` accepts a plain function or an `async def`. If the
  callback returns an awaitable, it's awaited on the same coroutine/thread
  as the I/O call it observed -- covers consumers doing async I/O inside
  their hook (shipping events to a remote collector, for example) without
  a second callback shape.
- `otel_on_io_adapter` takes an optional `meter` alongside `tracer`:
  spans are always emitted; a `duration` histogram, `bytes` counter, and
  `count` histogram (keys scanned by `find`/`find_detailed` -- the number
  behind this saver's one documented O(n) cost) are additionally recorded
  when a `meter` is passed.
- `FileNotFoundError` is expected control flow for this saver (an empty
  thread's first `get_tuple`/`list`, or deleting an already-gone thread),
  not a real failure. `otel_on_io_adapter` still records it on the span
  (`record_exception`) for context, but doesn't set the span's status to
  `ERROR` or the metrics' `error` attribute to `true` for it -- only for
  an exception that isn't `FileNotFoundError`. Getting this wrong would
  drown real failures in false positives for any alert keyed on error
  rate.
- A cancelled call's `_emit_io` (see the `BaseException` handling above)
  is scheduled as a fire-and-forget task rather than awaited inline when
  the captured error is `asyncio.CancelledError`, so a slow `on_io` can't
  delay the actual cancellation's unwind -- e.g. a `wait_for`-style
  timeout waiting on a checkpoint write shouldn't also end up waiting on
  an observability callback. The saver holds a strong reference to that
  task (`_background_io_tasks`) until it finishes, since asyncio can
  otherwise garbage-collect a pending task with no other referent.
- README's Observability section covers all of the above, plus
  exporter-configuration examples for Datadog, Prometheus, and Dynatrace
  (all consumer-side OTel exporter setup, not vendor-specific code in
  this library).

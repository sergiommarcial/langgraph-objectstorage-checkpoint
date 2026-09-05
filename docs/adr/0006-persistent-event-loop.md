# ADR 0006: Persistent background event loop for the non-async-native sync path

- **Status:** Proposed
- **Date:** 2026-09-05
- **Owners:** ObjectStorageSaver maintainers

## Context

Profiling `put`/`get_tuple` against local disk (cProfile, 2000 calls each)
showed roughly a third of wall time in `select.kqueue.control` alone,
plus real time in `_thread.lock.acquire` and `_thread.start_new_thread`,
on top of asyncio's own `_run_once`/`run_until_complete`/`run_forever`
machinery. All of that comes from `_run_sync` (saver.py:192) calling
`asyncio.run(func, *args, **kwargs)` for non-async-native filesystems
(local disk today; any fsspec backend that isn't an `AsyncFileSystem`):
a brand new event loop, and a fresh `asyncio.to_thread` executor thread,
get created and torn down on every single sync call. Actual business
logic (`_get_tuple` itself) accounted for a small fraction of that time.

Async-native backends (S3 via s3fs, GCS via gcsfs) don't have this
problem: `_run_sync`'s other branch already reuses a persistent loop the
filesystem itself owns (`self.fs.loop`) via `fsspec_sync`.

## Decision

Give non-async-native filesystems the same persistent-loop treatment
async-native ones already get, self-managed since there's no
filesystem-provided loop to borrow.

### Mechanism

- On the first sync call that needs it, `_run_sync` lazily creates a
  background thread running its own event loop (`asyncio.new_event_loop()`
  plus `loop.run_forever()`), and dispatches the call via
  `asyncio.run_coroutine_threadsafe(func(*args, **kwargs), loop).result()`
  instead of `asyncio.run(...)`.
- A `threading.Lock` guards that lazy creation, so two threads racing on
  the first sync call against the same instance don't each start their
  own loop.
- The thread is `daemon=True`, so it never blocks process exit even if
  the cleanup below doesn't run.

### Lifecycle

A `weakref.finalize(self, _stop_loop, loop, thread)` is registered when
the loop is created, stopping the loop
(`loop.call_soon_threadsafe(loop.stop)`) and joining the thread once the
saver instance is garbage collected. This is wrapped so a failure during
interpreter shutdown (module globals can be torn down in an unpredictable
order at that point) doesn't raise. It isn't just tidiness: this repo's
own test suite constructs a fresh local-disk saver in most unit tests, so
without cleanup a full `pytest` run would accumulate one live idle
background thread per test.

### Scope

Only `_run_sync`'s non-async-native branch changes. The async-native
branch (`fsspec_sync(self.fs.loop, ...)`) is untouched: it already
reuses a persistent loop, just one owned by the filesystem object instead
of the saver.

## Alternatives considered

### A. One process-wide shared background loop instead of one per saver instance

Would amortize even more for an application that creates many
short-lived saver instances.

Rejected: breaks the existing pattern where each filesystem/saver
instance owns its own loop (the async-native branch already works this
way via `self.fs.loop`), and puts unrelated saver instances' work on one
shared thread, where one slow operation can delay another's. Per-instance
keeps the isolation the async-native path already has.

### B. Trim `asyncio.run()`'s own bookkeeping instead of persisting a loop

Manually call `asyncio.new_event_loop()`, `loop.run_until_complete(...)`,
and `loop.close()` per call, skipping some of what `asyncio.run()` does
around task cleanup.

Rejected: profiling shows the dominant costs are kqueue
registration/teardown and fresh executor-thread creation, both of which
come from creating a *new* loop and a *new* default executor every call,
not from `asyncio.run()`'s specific extra bookkeeping. This wouldn't fix
the actual bottleneck.

## Consequences

**Positive**

- Removes per-call event-loop and thread-spawn overhead from every sync
  call against local disk (and any other non-async-native backend),
  which profiling showed as the majority of that call's wall time.
- No public API change: no `close()`, no context manager, nothing a
  consumer has to remember to call.
- Symmetric with how async-native backends already behave: every
  filesystem-backed saver now has exactly one persistent loop behind its
  sync API, not zero or two different models depending on backend.

**Negative / risks**

- Introduces the library's first background thread that can outlive a
  single call. `weakref.finalize`-based cleanup is best-effort:
  interpreter-shutdown ordering can be unpredictable, so this is wrapped
  defensively rather than assumed reliable. The daemon flag is the actual
  safety net for process exit; finalize is about not accumulating threads
  during a long-lived process (or a test run), not a correctness
  guarantee.
- A saver instance that's still reachable (not garbage collected) keeps
  its background thread alive even if idle. An application creating and
  discarding many local-disk saver instances without ever releasing all
  references would accumulate threads; the typical usage pattern (one
  saver instance for the app's lifetime) doesn't hit this.
- One more piece of concurrency machinery in `saver.py` to reason about
  and keep correct.

## Applicability

**Worth it for:**

- Any application making frequent sync calls against a local-disk (or
  other non-async-native) saver instance, where per-call loop/thread
  setup was previously a meaningful fraction of latency.

**Skip for:**

- Consumers exclusively using the async API (`aput`/`aget_tuple`/...)
  against a non-async-native filesystem, who never trigger
  `_run_sync`'s non-async-native branch in the first place.
- Consumers on S3/GCS, who were never affected by this cost to begin
  with.

## Follow-up / open questions

- Whether the same treatment is worth extending to any future
  non-async-native, non-local backend is left open until one actually
  exists.
- No change to the documented sync/async-mixing warning: it's specific
  to async-native backends' loop-bound aiohttp sessions, and local disk
  I/O has no such loop affinity, so this change doesn't introduce a new
  version of that hazard.

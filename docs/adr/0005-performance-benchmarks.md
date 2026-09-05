# ADR 0005: Baseline performance benchmark suite

- **Status:** Accepted (implemented)
- **Date:** 2026-09-05
- **Owners:** ObjectStorageSaver maintainers

## Context

No performance test infrastructure exists. The README's Known limitations
section already documents `list(filter=...)`'s O(n) client-side scan and
`get_tuple(latest)`'s full prefix listing on every resume, but backs both
with no real numbers. `tests/` proves correctness via the
`langgraph-checkpoint-conformance` suite and hand-written tests; it says
nothing about cost. There is also no way to see what turning on
compression ([ADR 0003](0003-checkpoint-compression.md)) or encryption
([ADR 0004](0004-checkpoint-encryption.md)) costs relative to a plain
envelope.

## Decision

Add `tests/benchmark/`, built on `pytest-benchmark`. It sits under
`tests/` so it inherits `tests/conftest.py`'s fixtures (the same
in-process moto server used elsewhere) for free, but `pyproject.toml`'s
`testpaths` is narrowed to `["tests/unit", "tests/integration"]` so plain
`pytest`, `make test`, and `make test-unit` never collect it. Running it
requires the explicit path: `pytest tests/benchmark`.

### Structure

- `tests/benchmark/conftest.py`: fixtures for a local-fs saver and an
  in-process moto-S3 saver, parametrized over envelope config (plain,
  compression, encryption).
- `tests/benchmark/bench_hotpath.py`: `put`, `get_tuple`, `put_writes`.
- `tests/benchmark/bench_scan_costs.py`: `list(filter=...)` and
  `get_tuple(latest)` at thread history sizes 10, 100, and 1000.
- `tests/benchmark/bench_payload_size.py`: small, medium, and large
  checkpoint payload sweep.
- `tests/benchmark/bench_delete_thread.py`: `delete_thread` cost against
  history size.
- `tests/benchmark/report.py`: turns the latest saved benchmark run into
  a markdown table and a standalone HTML report.
- `tests/benchmark/seed.py`: builds a chain of N linked checkpoints for
  the history-size benchmarks; correctness-tested in
  `tests/unit/test_benchmark_seed.py` rather than under `tests/benchmark`
  itself, since that test needs to run in the normal gating suite, not
  only when someone explicitly runs the benchmarks.

### Dimension matrix

Not a full cartesian product everywhere. That would multiply cases for
no real signal:

- Hot-path and payload-size benchmarks vary backend (local, moto-S3),
  envelope (plain, compression, encryption), and payload size.
- Scan-cost and delete-at-scale benchmarks vary backend and history size,
  with envelope fixed to plain. These measure listing and deletion cost,
  not serialization cost, so varying envelope on top would add rows
  without adding information.

### Running

- `make bench` runs `pytest tests/benchmark --benchmark-only
  --benchmark-autosave`. It depends only on `install`, not `lint`:
  benchmarks aren't a correctness gate and shouldn't fail on a style
  issue to be useful.
- `make bench-compare` runs pytest-benchmark's `compare` against the
  previous autosaved run, for a fast local regression signal during
  development.

### Reporting

`tests/benchmark/report.py` reads the latest autosaved JSON
(pytest-benchmark's own format under `.benchmarks/`, gitignored) and
produces two outputs from that one source of truth:

- A markdown table (operation, backend, envelope/payload-size/history-size,
  mean, standard deviation, ops per second) written into a "Performance"
  section in `README.md`, between `<!-- BENCHMARK-RESULTS:START -->` and
  `<!-- BENCHMARK-RESULTS:END -->` markers. It includes a line stating the
  numbers were measured on maintainer hardware on a given date: a relative
  comparison across operations, backends, and envelopes, not an absolute
  production guarantee.
- `tests/benchmark/report.html`, a self-contained HTML page (inline SVG
  bar chart, no JS dependency) for a shareable visual snapshot.

`make bench-report` runs `report.py` and updates the README in place.
This is a separate, manual, maintainer-run step, not wired into
`make bench` and not automated in CI. Refreshing published numbers is a
deliberate act, not something that happens as a side effect of whatever a
local run measured on that day's dev hardware.

### Dependencies

`pytest-benchmark` is added to `[dependency-groups] dev`, matching
`pytest`, `moto`, and `boto3`'s existing placement as dev/test-only
tooling. `report.py`'s HTML/SVG output needs no new dependency: a
hand-written inline SVG bar chart is enough for one chart type, and
pulling in a charting library for it would be the kind of premature
abstraction YAGNI rules out.

## Alternatives considered

### A. asv (airspeed velocity)

Purpose-built for tracking performance across git history, with
generated HTML dashboards.

Rejected for this phase. It needs its own machine-info and config model
separate from pytest, and that setup cost only pays off once there's a
CI regression-tracking phase and a baseline worth tracking across
commits. Neither exists yet. Nothing here rules out adopting it later.

### B. Hand-rolled `timeit` script instead of pytest-benchmark

Avoids one new dev dependency.

Rejected. It would reinvent statistical warm-up and outlier handling,
JSON output, and the autosave/compare workflow pytest-benchmark already
provides: exactly the kind of "build our own" YAGNI rules out when a
well-established tool already does the job.

### C. Fold `make bench-report`'s README refresh into `make bench`

Rejected. It would rewrite `README.md` on every local benchmark run, on
whatever hardware happened to run it, turning a documentation artifact
into a byproduct of dev-machine noise. A separate, deliberate step keeps
published numbers a maintainer decision.

## Consequences

**Positive**

- The README's documented O(n) costs (`list(filter=...)`,
  `get_tuple(latest)`) get real numbers instead of a qualitative caveat.
- Envelope overhead (compression, encryption) becomes measurable,
  directly answering the "what does turning this on cost me" question
  that follows from ADR 0003 and ADR 0004.
- `make bench` and `make bench-compare` give a fast local regression
  signal with no CI infrastructure changes required.
- `tests/benchmark` can't slow down or flake the correctness suite: it's
  outside `testpaths`, so `make test` never touches it.

**Negative / risks**

- Numbers from moto (in-process S3 emulation) don't reflect real S3
  network latency. They're useful for relative comparison, backend
  against backend or envelope on against off, not as an absolute
  production number. The README's caveat line exists specifically so
  these aren't misread as SLA-grade.
- The README's Performance section can go stale if `make bench-report`
  isn't re-run before a release. Nothing enforces freshness in this
  phase; that's the deferred CI-gating phase's job (see Follow-up).
- One more dev dependency, `pytest-benchmark`, to keep current.

## Applicability

**Worth enabling for:**

- Anyone changing `saver.py`'s I/O bridge, `envelope.py`'s pack/unpack,
  or `keys.py`'s listing/path logic, who wants to see whether a change
  moved the needle.
- Documenting real relative costs for the README's Known limitations
  section.

**Skip for:**

- Gating a PR on performance. That's not what this phase does (see
  Follow-up).
- Treating these numbers as absolute production SLAs. They're relative,
  dev-machine/emulator measurements.

## Follow-up / open questions

- `.github/workflows/ci.yml` has a `bench` job that runs `make bench` and
  uploads `.benchmarks/` as a build artifact on every push and PR. It's
  informational only: it doesn't gate anything, isn't a `release`
  dependency, and doesn't touch `README.md` (that stays the manual
  `make bench-report` step below). This doesn't resolve the item below,
  it just gives CI-run numbers to eventually build a baseline from.
- CI regression gating (failing a PR past some threshold) is a separate
  future ADR. It needs accumulated baseline data first, and a decision on
  handling CI-runner/moto noise before a hard threshold is trustworthy.
- Concurrency/load testing (many concurrent readers/writers, contention
  or throttling) is a different failure mode from per-operation latency,
  so it's out of scope here and would need its own design.
- GCS (`fake-gcs-server`) is left out of the default `make bench` matrix,
  opt-in via `docker compose` like `test-integration`. Add it if S3
  numbers don't generalize.
- Whether the README refresh should ever become CI-automated (a
  scheduled job, say) instead of manual is left open, deferred until the
  CI-gating follow-up above is designed.

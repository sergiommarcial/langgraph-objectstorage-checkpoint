# ADR 0001: Optional SlateDB backend to bound `get_tuple(latest)` and `list()` cost as checkpoint count grows

- **Status:** Proposed
- **Date:** 2026-08-30
- **Owners:** ObjectStorageSaver maintainers

## Context

`ObjectStorageSaver` stores each checkpoint and each pending write as its
own object under `root/thread_id/checkpoint_ns/...` (`keys.py`). There's
no index, no sidecar, no local state of any kind. So every read that
isn't an exact `(thread_id, checkpoint_ns, checkpoint_id)` lookup falls
back to listing the object store and scanning the result:

- `get_tuple(config)` without an explicit `checkpoint_id` (i.e. "give me
  the latest checkpoint for this thread") calls
  `self._find(checkpoints_prefix(...))` and takes `max(candidates)`
  (`saver.py:194-211`). This is the resume hot path. It runs on every
  graph invocation that continues an existing thread.
- `list()` / `alist()` calls the same `_find(prefix)`, sorts client-side,
  then for each candidate checkpoint issues a `_cat` (to read/unpack it
  for `filter`) plus a second `_find` per checkpoint to read its pending
  writes (`saver.py:263-306`, `_read_pending_writes` at
  `saver.py:163-176`).

`_find` is a full prefix listing (`fs.find`), backed by paginated
`ListObjectsV2`-style calls on S3/GCS. Cost and latency scale with the
number of keys under the prefix, not with payload size, and each page is
a billed, network-bound round trip. The `list(filter=...)` O(n) scan is
already an accepted, documented non-goal (README "Known limitations",
CLAUDE.md). But `get_tuple(latest)` paying that same full-prefix-listing
cost isn't something a consumer opts into. It happens on every resume,
silently, and it gets worse the longer a thread's history gets (long
loops, heavy retries, branching/time-travel workloads).

This isn't a gradual slowdown, it's a cliff. Once a thread's checkpoint
count outgrows a handful of list pages, the steady-state resume path
(today, one `GET`) turns into a paginated `LIST` scan plus a `GET`. And
`list()` turns into `O(n)` `LIST` + `O(n)` `GET` + `O(n)` nested `LIST`
calls.

### Constraint: this is a published OSS library

Unlike an internal service, we can't unilaterally change on-disk format,
public API shape, or dependency footprint:

- Object keys under `root` (`keys.py`) are a de facto on-disk format.
  Existing installations already have data written in this layout. Any
  change has to be transparent to that existing data, or ship with an
  explicit, documented migration. Never a silent format break.
- `ObjectStorageSaver.__init__(fs, root)` and `from_conn_string(...)` are
  public constructor API. A fix can't require existing callers to change
  how they construct the saver, and can't change default behavior in a
  way that surprises someone who upgrades a patch or minor version.
- Core dependencies are deliberately backend-agnostic (`fsspec` only, per
  CLAUDE.md's packaging conventions). Adding a new hard dependency is a
  bigger call than it would be for an internal tool, since it lands on
  every consumer of the library, including ones on backends we can't
  test ourselves.
- Whatever ships still has to pass the `BaseCheckpointSaver` conformance
  suite (`report.passed_all_base()`) unchanged for every backend,
  existing or new.
- CLAUDE.md's own non-goals rule out GC, cross-checkpoint transactions,
  and locking between same-thread concurrent writers. Anything proposed
  here has to either respect those or say plainly that it's asking to
  reopen one, and why.

## Decision

Add SlateDB as a second, opt-in storage backend behind a new `slatedb`
extra, alongside the existing fsspec-backed one. Default behavior for
every current consumer stays exactly as it is today: nobody is moved
onto SlateDB, nothing about the local/S3/GCS-via-fsspec path changes,
and no existing data needs migrating. A consumer who wants the SlateDB
path chooses it explicitly at construction (e.g. a `slatedb+s3://` style
connection string, or an equivalent constructor argument), the same way
choosing S3 vs GCS vs local is a construction-time choice today.

SlateDB is an embedded LSM engine that writes its memtable flushes and
compacted SSTs to object storage (S3, GCS, Azure Blob, MinIO). For a
thread whose checkpoint history is large, its sorted, compacted key
index turns "latest checkpoint for thread X" into an O(log n) point
lookup and `list()`'s `before`/`limit` into a bounded range scan,
neither of which needs a listing call at all. That's a structural fix
for the problem in Context, not a workaround for it, and it comes from a
maintained engine instead of hand-rolled index bookkeeping this project
would otherwise own and debug itself.

### How it fits the existing architecture

`ObjectStorageSaver` stays the one public class. Internally, `saver.py`'s
`_cat`/`_pipe`/`_find`/`_exists`/`_rm` bridge becomes a small internal
storage-backend interface with two implementations: the current
fsspec-backed one, and a new SlateDB-backed one. `saver.py`'s
business-logic methods (`_get_tuple`/`_put`/`_list`/`_put_writes`/
`_delete_thread`) call the interface, not either backend directly, so
this is additive to the async-core/sync-wrapper pattern already in
place, not a parallel rewrite of it. `from_conn_string` picks the
backend from the connection string's scheme, same as it picks an
fsspec filesystem today. This does mean CLAUDE.md's architecture section
needs a short update once this ships, to describe the backend interface
alongside the "one class, backend chosen at construction" rule it
already states.

### What changes for someone who opts in

A few real tradeoffs come with choosing this backend, worth stating
plainly instead of burying them:

- SlateDB is designed for one writer per database (fencing is available
  to partition beyond that). The current fsspec-backed default tolerates
  concurrent writers per thread by relying on unique keys, not locks;
  SlateDB enforces its own writer discipline instead. A consumer needs
  to know that before opting in, particularly in multi-process
  deployments. Because it's opt-in, this doesn't touch the "no locking
  between same-thread concurrent writers" non-goal for anyone who stays
  on the default backend, and for SlateDB itself we're relying on its
  built-in fencing rather than building locking ourselves.
- Checkpoints stop being individually inspectable objects (`aws s3 ls` /
  direct GET, which works today) and become entries inside SSTs,
  readable only through SlateDB's own client or tooling. Worth calling
  out plainly in the docs for this backend.
- It's a new dependency: a Rust-core binding, gated behind its own extra
  (`slatedb`), following the same pattern as the existing `s3`/`gcs`
  extras. Core deps stay `fsspec`-only, so nobody who doesn't opt in
  installs it.
- Moving an existing thread's history from the fsspec-backed layout into
  SlateDB isn't addressed here. If that's wanted later, it's a separate,
  explicit export/import tool, not something this ADR ships.

## Alternatives considered

### A. Advisory sidecar index on the existing fsspec backend

Add a small per-`(thread_id, checkpoint_ns)` index object (e.g.
`index.msgpack`) listing known `checkpoint_id`s, written best-effort on
`put` and read by `get_tuple(latest)`/`list()` instead of `_find`. If
it's missing, stale, or unreadable, fall back to today's scan and
self-heal the index from that scan's result. Correctness never depends
on the index being present or race-free.

This is the smaller, cheaper fix: no new dependency, no format break,
no single-writer constraint, ships as a default-on minor-version change
because it's fully backward compatible (a missing index degrades to
exactly current behavior). It's the better choice if the goal is purely
"stop `get_tuple(latest)`/`list()` from scaling with checkpoint count"
with the least possible change to what's already shipped.

Why it's the alternative here rather than the decision: it only ever
gets as good as "avoid the listing call." It doesn't give transactional
writes, doesn't give a real range-query engine, and every correctness
edge case around concurrent index updates (lost writes under races,
staleness windows) is something this project would design, implement,
and maintain by hand. SlateDB gets the same core win plus headroom for
things this project might want later (atomic `put` + `put_writes`,
atomic `delete_thread`) from an engine that already handles the hard
parts. If the team would rather ship the smallest possible fix first
and revisit SlateDB later, this is the fallback path, and the two aren't
mutually exclusive: the sidecar index could still ship for the default
backend even after SlateDB support lands, since it costs nothing for
consumers who don't opt into SlateDB.

### B. Do nothing (status quo)

`list(filter=...)`'s O(n) cost is already documented as an accepted
non-goal, so you could argue `get_tuple(latest)` paying full-scan cost
is more of the same. Rejected because `get_tuple(latest)` isn't opt-in
the way `filter` is. It's the default resume path, and its cost growing
unbounded with thread history is closer to a correctness problem than a
documented trade-off a caller chose (timeouts, cost blowups at scale).

### C. External index in a real database (SQLite sidecar, DynamoDB, etc.)

Would give strong consistency and real query capability, but introduces
an operational dependency existing consumers don't have today (a second
system to provision and reach) and breaks the "single fsspec URI"
simplicity of `from_conn_string`. Rejected: it asks consumers for a new
hard external dependency, same as SlateDB, but without SlateDB's
advantage of still being embedded and object-storage-backed rather than
a separately hosted service.

## Consequences

**Positive**

- Threads that opt into the SlateDB backend get `get_tuple(latest)` and
  `list()` costs that don't scale with total checkpoint count: O(log n)
  point lookup and bounded range scans instead of full prefix listings.
- Zero behavior change for anyone who doesn't opt in. No migration, no
  new required dependency, no API change to the default path.
- Headroom for capabilities the project doesn't need today but might
  (atomic multi-key writes, real range queries) without having to build
  and maintain that machinery in-house.
- Fits the existing single-class, construction-time-backend-choice
  architecture as an additive internal interface, not a rewrite.

**Negative / risks**

- Two storage-backend implementations to maintain and keep behaviorally
  identical against the conformance suite, instead of one.
- Single-writer constraint is a real, documented behavior difference for
  anyone opting in, and a source of support burden if it's not
  explained clearly (multi-process deployments especially).
- Opaque per-thread storage for SlateDB-backed threads: no more direct
  object inspection via the cloud console or CLI.
- New dependency class (Rust binding) to track for security/version
  updates, even though it's opt-in.
- No migration path from existing fsspec-backed data to SlateDB is
  provided by this ADR; consumers who want to switch an existing
  thread's history over have no supported way to do it yet.

## Applicability

**Worth choosing the SlateDB backend for:**

- New deployments, or new threads, expected to accumulate large
  checkpoint histories (long-running agent loops, heavy retries,
  branching/time-travel workloads) where `get_tuple(latest)`/`list()`
  cost is known to matter from the start.
- Consumers who can guarantee single-writer-per-thread (or can use
  SlateDB's fencing) without disruption to their deployment model.
- Teams that anticipate wanting atomic multi-key writes later and would
  rather not migrate storage engines twice.

**Stay on the default fsspec-backed path for:**

- Existing installations, unless and until there's a supported
  migration tool.
- Short-lived threads with few checkpoints, where listing cost is
  already negligible.
- Multi-process deployments that can't easily guarantee SlateDB's
  single-writer discipline.
- Anyone who wants direct, tool-agnostic inspection of individual
  checkpoint objects in the bucket.

## Follow-up / open questions

- Exact shape of the internal storage-backend interface (`saver.py`'s
  bridge methods, generalized) needs its own design pass before
  implementation starts.
- Connection-string scheme for selecting SlateDB at construction (e.g.
  `slatedb+s3://...`) needs to be settled and documented alongside the
  existing `from_conn_string` examples.
- Whether the advisory sidecar index (Alternative A) ships too, for the
  default backend, is a separate decision and not blocked by this one.
- A cross-backend migration tool (fsspec-backed history → SlateDB) is
  explicitly out of scope here; worth its own ADR if demand shows up.
- CLAUDE.md's architecture section needs a short update once this ships
  to describe the internal backend interface.

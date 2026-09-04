# ADR 0001: Age-based checkpoint TTL via native cloud lifecycle rules, explicit sweep on local disk

- **Status:** Accepted (implemented)
- **Date:** 2026-09-03
- **Owners:** ObjectStorageSaver maintainers

## Context

`ObjectStorageSaver` writes each checkpoint and each pending write as its
own immutable object under `root/thread_id/checkpoint_ns/...` (`keys.py`).
Nothing is ever overwritten, and the only removal path is `delete_thread`,
which a consumer has to call explicitly. Left alone, storage grows without
bound for the lifetime of the deployment.

CLAUDE.md lists garbage collection/retention as an explicit non-goal of
the original design, with the caveat: "If a real need for one of those
shows up, that's a new spec discussion, not a quiet addition." This ADR
is that discussion. Unbounded growth is a real operational cost for any
long-lived deployment, and "call `delete_thread` yourself, forever" isn't
a complete answer for threads that should simply age out.

## Decision

Add a single `ttl: timedelta | None` parameter to
`ObjectStorageSaver.__init__`/`from_conn_string`: one value per saver
instance, no per-checkpoint or per-thread override. `ttl=None` (the
default) disables the feature entirely; nothing about existing behavior
changes for anyone who doesn't set it.

Enforcement is backend-specific, on purpose:

- **S3 and GCS:** no code path at all. Every object a saver instance
  writes already lives under one `root` prefix. A bucket lifecycle rule
  the operator configures, filtered by that same prefix, expires
  everything the saver owns on the cloud provider's own schedule
  (typically once a day). Zero write-path changes: no per-object
  tagging, no extra API calls on `put`/`put_writes`.
- **Local filesystem** (and optionally S3/GCS too, as an alternative to
  waiting on the cloud provider's cadence): new `delete_expired()` /
  `adelete_expired()` methods, following the same async-core/sync-wrapper
  split as every other operation in `saver.py`. One `fs.find(root,
  detail=True)` pass, remove every object whose mtime is older than
  `now - ttl` via the existing `_rm` bridge. Caller-triggered only (cron,
  k8s CronJob, ...). The saver never spawns a background thread or timer.

## Alternatives considered

### A. Active sweep/delete in the saver as the only mechanism

Skip the native-lifecycle path; every backend, including S3/GCS, relies
on `delete_expired()` being scheduled externally.

Rejected: it throws away a mechanism S3/GCS already provide for free, and
forces every cloud consumer into caller-scheduled sweeps for a job the
storage provider will do on its own if just told the prefix and the age.

### B. Read-time filtering instead of deletion

`get_tuple`/`list` silently skip checkpoints past their TTL, without
actually removing them.

Rejected: doesn't bound storage growth at all, just hides it from reads.
The stated goal was auto-*deletion*, not auto-*hiding*.

### C. Per-object tagging plus tag-filtered lifecycle rules

Tag every object at write time (S3 object tags / a GCS equivalent) so an
operator's lifecycle rule can target a specific tag value instead of a
plain prefix.

Rejected once TTL was scoped to one value per saver instance: a
prefix-filtered rule gets the same isolation with zero write-path
changes. Tagging would add a `PutObjectTagging`-equivalent call to every
`put`/`put_writes` for no behavioral gain at that granularity. That's the
kind of premature complexity YAGNI rules out.

### D. Saver provisions the lifecycle rule itself

Have the saver call `put_bucket_lifecycle_configuration` (S3) or the
equivalent GCS API at construction time, given `ttl`.

Rejected: this means the saver mutating bucket-level configuration
outside its own object namespace, needing IAM permissions well beyond
read/write on its own keys. It keeps the saver in its lane: I/O on
`root`, never bucket administration. That matches the existing rule that
backend-specific quirks stay behind the `_cat`/`_pipe`/`_find`/`_exists`/
`_rm` bridge, not layered on top of it as new responsibilities.

### E. Opportunistic sweep triggered probabilistically on `put()`

Give every `put()` call a small random chance of triggering a sweep pass,
so no external scheduler is needed.

Rejected: hides an O(n) `fs.find()` inside what looks like a plain write,
makes deletion timing nondeterministic, and is harder to test and reason
about than an explicit, caller-triggered method. Also cuts against
CLAUDE.md's existing stance of no hidden background coordination.

## Consequences

**Positive**

- Zero-cost, zero-code TTL for S3/GCS: a config the operator already
  knows how to write, no saver code involved, no ongoing compute.
- Local disk gets parity through one small, explicit, well-tested method:
  no new dependency, no background thread, no hidden scan inside a hot
  path.
- Reuses the exact `_find`/`_rm` I/O bridge every other operation already
  goes through; no new backend-specific code paths to maintain.

**Negative / risks**

- Per-object age, not per-checkpoint-chain: a checkpoint's `writes`
  objects are added later (via `put_writes`) and age out on their own
  clock, so they can expire slightly before or after the checkpoint they
  belong to. Not fixed: doing so would need an O(n·m) chain walk the
  original spec already avoided for the hot path.
- No read-time filtering: an object past its TTL is still returned by
  `get_tuple`/`list` until a lifecycle run or `delete_expired()` call
  actually removes it.
- Shared-bucket footgun, on the *operator's* side: S3's `Filter.Prefix`
  and GCS's `matchesPrefix` are literal string matches, so a lifecycle
  rule missing the trailing `/` also matches a sibling like
  `{root}-backup/...`, silently expiring unrelated data. Verified this is
  not a bug in `delete_expired()` itself: its `fs.find()`/`fs.rm()` calls
  treat `root` as a real directory path on local disk, s3fs, and gcsfs
  alike (regression-tested in `tests/unit/test_ttl.py` and
  `tests/integration/test_ttl_{s3,gcs}.py`). README documents the
  trailing-slash requirement explicitly.
- Clock-skew hazard is real only against local emulator containers
  (moto-server, fake-gcs-server), not production S3/GCS, whose clocks are
  NTP-synced against the same time the caller's machine is. Documented so
  nobody "fixes" a production deployment that was never actually at risk.

## Applicability

**Worth setting `ttl` for:**

- Any deployment where checkpoint storage cost or object count needs a
  ceiling over time, rather than relying entirely on explicit
  `delete_thread` calls.
- S3/GCS-backed deployments especially, since the lifecycle-rule path
  costs nothing to operate once configured.

**Skip it (leave `ttl=None`) for:**

- Deployments that already manage retention entirely through
  `delete_thread` at a point they control precisely (e.g. on explicit
  session end), where age-based expiry would add a second, looser
  retention mechanism on top.
- Anyone who needs per-thread or per-checkpoint retention windows: this
  ADR deliberately scopes TTL to one value per saver instance.

## Follow-up / open questions

- No fixup logic is planned for the per-object-age skew between a
  checkpoint and its writes; revisit only if a concrete need shows up.
- No tooling to auto-provision or validate the operator's bucket
  lifecycle rule from Python; it stays a documented manual step (README
  "Checkpoint TTL" section). The one mitigation that did ship:
  `__init__` logs a one-time `WARNING` whenever `ttl` is set, naming both
  the lifecycle-rule and `delete_expired()` paths. Cheap (no extra API
  calls or permissions), since actually verifying a rule exists would
  mean the saver reading bucket-level config it doesn't otherwise touch,
  which Alternative D above already rejected for the provisioning case.

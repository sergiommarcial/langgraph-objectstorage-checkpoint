# ADR 0003: Pluggable payload compression for checkpoint and write envelopes

- **Status:** Accepted (implemented)
- **Date:** 2026-09-04
- **Owners:** ObjectStorageSaver maintainers

## Context

`envelope.py` packs each checkpoint and each pending write into an
`ormsgpack` blob before `saver.py` hands it to `_pipe` for upload
(`pack_checkpoint`/`pack_write`, `envelope.py:12-53`). That blob is never
compressed. For graph state with large channel values (accumulated
message history, embeddings, big tool outputs), the on-disk object is the
same size as the serialized Python object, and every `_cat` on read pays
for transferring that full size over the network on S3/GCS.

This is a storage-cost and transfer-latency question, and it's orthogonal
to the listing-cost problem in [ADR 0002](0002-slatedb.md): that ADR is
about the *number* of round trips scaling with checkpoint count, this one
is about the *size* of each round trip scaling with payload size. Fixing
one doesn't fix the other, and a long-running thread with large state can
hit both at once.

### Constraint: this is a published OSS library

Same constraints as ADR 0002 apply here:

- Object bytes under `root` are a de facto on-disk format. Existing
  installations have uncompressed objects already written. Any change
  has to read that data unchanged, forever, not just until some
  migration window closes.
- `ObjectStorageSaver.__init__`/`from_conn_string` are public
  constructor API; a new option can't change default behavior for
  someone who doesn't ask for it.
- Core dependencies stay backend-agnostic (`fsspec` only). A codec that
  needs a new hard dependency has to be opt-in, following the existing
  `s3`/`gcs` extras pattern, not added to core.
- Must still pass `report.passed_all_base()` for every backend, with
  compression on and off.

## Decision

Add a `compression` option to `ObjectStorageSaver.__init__` and
`from_conn_string` (as a query parameter, e.g.
`file:///path?compression=lzma`), accepting `"none"` (default),
`"zlib"`, `"lzma"`, or `"zstd"`. `"none"` preserves today's behavior
exactly: this is an explicit opt-in, not a new default. `zlib` and
`lzma` use Python's standard library, so no new dependency; `zstd` is
gated behind a new `compression` extra (pulls in `zstandard`), following
the same pattern as the `s3`/`gcs` extras. Requesting
`compression="zstd"` without the extra installed raises a clear
`ImportError` at construction time instead of a confusing failure buried
in a later `put`/`get_tuple` call.

### Wire format

`envelope.py`'s pack functions already wrap the serialized checkpoint and
metadata in one `ormsgpack` dict
(`{"checkpoint": ..., "metadata": ..., "parent_checkpoint_id": ...}`).
When compression is enabled, that packed dict's bytes become the
*payload* of one more `ormsgpack` wrapper:

```python
inner = ormsgpack.packb({"checkpoint": [...], "metadata": [...], "parent_checkpoint_id": ...})
outer = _wrap(inner, codec_name="lzma")
```

Reading always does one `ormsgpack.unpackb` first, on the result of
`_unwrap`. `ormsgpack` is self-describing, so the resulting dict's keys
tell you which shape you got: a `"codec"` key means decompress
`"payload"` and unpack again to get the original checkpoint/metadata
dict, and no `"codec"` key means it's today's legacy, uncompressed dict,
used as-is. `pack_write`/`unpack_write` get identical treatment through
the same `_wrap`/`_unwrap` pair, instead of duplicating the wrap/unwrap
logic for checkpoints and writes separately.

That keeps the change inside `envelope.py`'s existing "pack into one
opaque msgpack blob" shape, rather than introducing a second, parallel
framing mechanism (a raw magic-byte header) alongside it. See
Alternative A for why that was the other option on the table, and why it
lost out.

`compression="none"` output stays byte-for-byte identical to today's:
no wrapper, no format change, nothing for an existing consumer to
notice on upgrade.

### Internal codec registry

`_wrap`/`_unwrap` dispatch through a small `Codec` protocol and a
`dict[str, Codec]` registry, rather than an `if codec == "zlib": ... elif
"lzma": ...` chain repeated in every pack/unpack function:

```python
class Codec(Protocol):
    def compress(self, data: bytes) -> bytes: ...
    def decompress(self, data: bytes) -> bytes: ...

_CODECS: dict[str, Codec] = {"zlib": _Zlib(), "lzma": _Lzma()}
# "zstd" registered only if the `compression` extra's `zstandard` import succeeds

def _wrap(data: bytes, codec_name: str | None) -> bytes:
    if codec_name is None:
        return data
    return ormsgpack.packb({"codec": codec_name, "payload": _CODECS[codec_name].compress(data)})

def _unwrap(data: bytes) -> bytes:
    obj = ormsgpack.unpackb(data)
    if isinstance(obj, dict) and "codec" in obj:
        return _CODECS[obj["codec"]].decompress(obj["payload"])
    return data
```

This is a Strategy pattern applied at the smallest scope that earns it:
one `Codec` implementation per algorithm, selected by name, with the
wrap/unwrap control flow written exactly once. Adding a codec later means
adding one class and one registry entry, not touching `pack_checkpoint`,
`unpack_checkpoint`, `pack_write`, or `unpack_write` at all.

### Mixed-codec buckets

A bucket's contents aren't required to share one codec. A saver instance
writes with whatever `compression` it was constructed with, but reads
decode using the `"codec"` field recorded *in the object itself*,
independent of the reading saver's own configured codec:

- Changing `compression=` between deploys of the same application is
  safe. Old objects stay readable under their original codec, and new
  objects use the new one.
- An unrecognized `"codec"` value on read (say, an object written by a
  newer version of this library with a codec this version doesn't know)
  fails fast with an error naming the unknown codec id, rather than
  silently returning corrupt data.

### Event-loop blocking on large payloads

`pack_checkpoint`/`unpack_checkpoint`/`pack_write`/`unpack_write` run as
plain synchronous calls inside `saver.py`'s `async def _put`/`_get_tuple`/
`_list`/`_put_writes`. Compression and decompression are CPU-bound, not
I/O, so calling `zlib.compress`/`lzma.compress`/`zstd.compress` directly
inside one of those coroutines blocks the event loop for the full
duration of the call. On a saver whose event loop is serving several
concurrent async operations at once (the exact scenario the README's
sync/async-loop-mismatch warning already exists for), a large
checkpoint's compression time is time every other concurrent
`aput`/`aget_tuple` on that loop spends waiting, not just the caller.

This ADR's decision is to call the codec inline, synchronously, the same
way `_serde.dumps_typed`/`loads_typed` already run inline in
`envelope.py` today. Offloading to a thread (`asyncio.to_thread`) would
remove the blocking, but adds a real thread-handoff cost to *every* call,
including the common case of a small checkpoint that compresses in low
single-digit milliseconds. Given typical checkpoint sizes, that tradeoff
isn't worth taking as the default. A consumer whose payloads are large
enough for this to matter is also the consumer most likely to benchmark
before opting into compression at all (see What changes for someone who
opts in, above). See Follow-up for a size-threshold-gated offload as a
possible later refinement.

### How it fits the existing architecture

`keys.py` is untouched. This is purely a change to what bytes
`envelope.py` produces and consumes; key layout and `_find`/`_exists`
prefix logic in `saver.py` never look at object contents, so they don't
change. `saver.py`'s business-logic methods keep calling
`pack_checkpoint`/`unpack_checkpoint`/`pack_write`/`unpack_write`
exactly as today; the codec is threaded through `ObjectStorageSaver`'s
constructor into those calls, not decided per-call. This is additive to
the existing `keys.py`/`envelope.py`/`saver.py` separation of concerns
(CLAUDE.md's Architecture conventions): no new module needed, and no
existing module's responsibility changes.

### What changes for someone who opts in

- Storage cost and transfer size drop for compressible payloads: text,
  JSON-like structures, repeated tokens. The actual ratio depends on
  content, and embeddings or already-compressed binary blobs won't
  shrink much.
- CPU cost lands on every `put`/`put_writes`/`get_tuple`/`list` call,
  proportional to payload size. `lzma` compresses and decompresses more
  slowly than `zlib`; `zstd` is faster than both at comparable or better
  ratios but costs a new dependency. Pick a codec against your own
  latency budget rather than assuming more compression is free.
- Checkpoint objects are no longer directly human-inspectable via
  `cat`/`aws s3 cp` without a decompression step once compression is
  enabled. That's a small but real loss of the plain-msgpack
  inspectability the format has today, worth documenting rather than
  glossing over.
- Very small payloads can net-expand under compression, since codec
  framing and dictionary overhead can exceed the savings on tiny inputs.
  This ADR doesn't add a size-threshold skip; see Follow-up.

## Alternatives considered

### A. Raw magic-byte header prepended to the object bytes

Instead of nesting the compressed payload inside another `ormsgpack`
dict, prepend a small fixed-size binary header directly to the object's
bytes, such as a 4-byte magic sequence plus a 1-byte codec id, followed
by either the raw legacy `ormsgpack` blob (uncompressed) or the
compressed bytes. The read path peeks at the first bytes: a
magic-sequence match means new format, anything else means legacy.

This is the more conventional approach for binary formats (it's how
gzip, zip, and most compressed container formats self-identify), and it
avoids paying for one extra `ormsgpack` unpack in the uncompressed case.

Rejected in favor of the nested-`ormsgpack` approach because it
introduces a second framing mechanism (raw byte layout) alongside the
one `envelope.py` already has (msgpack dicts), for a marginal
performance difference. Keeping everything inside one `ormsgpack`
structure means `envelope.py` has exactly one way of describing "what
shape is this blob," which is easier to reason about and extend later.
Adding per-object metadata beyond just a codec id costs nothing extra
under this approach: it's another dict key, not a header-format
revision.

### B. Encode codec in the object key instead of its bytes

Suffix compressed object keys (e.g. `....zlib`) instead of marking the
codec inside the bytes, so `_find` results alone reveal which objects
need decompression.

Rejected: `keys.py`'s layout is explicitly called out in ADR 0002 as a
de facto on-disk format that changes should avoid touching. Suffixing
keys means every consumer of `_find`'s output (prefix matching,
`before`/`limit` sorting in `_list`) has to account for a new key shape,
a much larger blast radius than a change contained entirely inside
`envelope.py`.

### C. Do nothing (status quo)

No compression stays simplest and keeps every object trivially
inspectable. Rejected as the sole path forward because it leaves
storage cost and transfer latency on the table for consumers with large
per-checkpoint state, who today have no way to opt into that tradeoff
even if they'd take it. Compression staying fully opt-in
(`"none"` default) means this alternative is still exactly what an
uninterested consumer gets. This ADR only adds a choice, not a
requirement.

## Consequences

**Positive**

- Consumers with large checkpoint payloads get a measurable reduction in
  storage cost and network transfer time, chosen explicitly per their
  own latency/CPU tradeoff.
- Zero behavior change for anyone who doesn't opt in: `"none"` output is
  byte-identical to today.
- Fits inside the existing `envelope.py` module boundary. No new module,
  no change to `keys.py` or the `_find`/`_exists`/`_rm` bridge logic in
  `saver.py`.
- Per-object codec recording means a fleet can change codecs over time
  without a migration step. Old and new objects coexist and both stay
  readable.

**Negative / risks**

- CPU cost added to every read/write path, proportional to payload size
  and codec choice. A consumer who opts in without benchmarking their
  own workload could regress latency instead of improving it.
- Compression/decompression runs inline on the event loop (see Event-loop
  blocking on large payloads, above), so a large payload's compression
  time is time every other concurrent async operation on that loop
  spends waiting, not just the caller's own latency.
- Compressed objects lose plain-`cat` inspectability, a property the
  format has today.
- One more dependency class (`zstandard`) to track for security/version
  updates, for consumers who opt into `zstd`, even though it's behind
  its own extra.
- Small-payload net-expansion isn't addressed by this ADR (see
  Follow-up). A consumer with many tiny checkpoints who turns on
  compression indiscriminately could see slightly larger objects, not
  smaller.

## Applicability

**Worth enabling compression for:**

- Threads carrying large, compressible state (accumulated message
  history, JSON-like tool outputs, long text) where storage cost or
  transfer latency is measurable today.
- Deployments willing to spend some CPU per checkpoint operation for
  lower storage/network cost: `zlib` or `zstd` for latency-sensitive
  paths, `lzma` where ratio matters more than speed.

**Stay on `compression="none"` for:**

- Existing installations, unless benchmarking shows it's worth the CPU
  cost. Nothing forces a migration.
- Workloads with small checkpoints, or state that's already
  compressed/high-entropy (embeddings, binary blobs), where compression
  buys little or nothing.
- Anyone relying on direct object inspection (`cat`, `aws s3 cp`) as
  part of their debugging workflow.

## Follow-up / open questions

- Size-threshold skip (don't compress payloads under some byte
  threshold, to avoid net-expansion on tiny checkpoints) is a separate
  knob, deliberately left out of this ADR's scope. Revisit once there's
  real payload-size data from consumers.
- The same threshold, if it ships, could also gate an `asyncio.to_thread`
  offload for payloads above it, avoiding the per-call thread-handoff
  cost for the common small-checkpoint case while still protecting the
  event loop from large ones. Left as a follow-up rather than this ADR's
  default, per Event-loop blocking on large payloads, above.
- Exact `from_conn_string` query-parameter name and accepted values
  (`compression=lzma` vs. an alternate spelling) need to be settled and
  documented alongside the existing connection-string examples.
- Whether `list(filter=...)`'s client-side scan should decompress lazily
  (stop after finding enough matches) or eagerly (decompress every
  candidate up front) is an implementation detail for the eventual plan,
  not a design fork this ADR needs to settle.
- README/CLAUDE.md need a short update once this ships: a new
  "Compression" section (mirroring the existing "Checkpoint TTL"
  section) and a packaging-conventions note for the new `compression`
  extra.

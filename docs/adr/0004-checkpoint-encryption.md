# ADR 0004: Pluggable client-side encryption for checkpoint and write envelopes

- **Status:** Accepted (implemented)
- **Date:** 2026-09-04
- **Owners:** ObjectStorageSaver maintainers

## Context

Checkpoints and pending writes go through `envelope.py`'s pack/unpack as
plain `ormsgpack` bytes (optionally compressed, once
[ADR 0003](0003-checkpoint-compression.md) ships) with no confidentiality
guarantee beyond whatever bucket-level IAM or ACLs a deployment happens
to configure. Graph state can carry PII, credentials, or other sensitive
values, and object storage buckets are routinely shared across teams or
services with broader read access than any single application would
choose on its own. The README already warns about this class of problem
for a different reason (a shared bucket's lifecycle rules silently
deleting another team's checkpoints); the same shared-bucket reality also
means anyone with read access to the bucket can read plaintext state
today.

A consumer who wants confidentiality at rest currently has to build it
outside the library entirely, for example by wrapping `fs` in something
that encrypts before every write. That's real friction for a use case the
library could support directly.

This ADR is scoped to confidentiality of the object bytes this library
writes. Bucket-level access control, transport encryption (TLS to
S3/GCS), and running actual key management infrastructure (a KMS, Vault,
etc.) all stay outside its scope, the same way ADR 0002 draws a line
around garbage collection and locking.

It's also scoped to payload confidentiality only. Object paths
(`keys.py`, untouched by this ADR) still encode `thread_id`,
`checkpoint_ns`, `checkpoint_id`, and task/channel names in plaintext.
Anyone with bucket read access still sees thread activity patterns,
checkpoint cadence, and task shape even with encryption on. This ADR
narrows "anyone with read access can read plaintext state" to payload
content, not existence/shape/timing metadata.

### Constraint: this is a published OSS library

Same constraints as ADR 0002 and ADR 0003 apply, plus encryption-specific
ones:

- A new option can't change default behavior for someone who doesn't ask
  for it, same as compression's `"none"` default.
- Core dependencies stay backend-agnostic (`fsspec` only); a crypto
  primitive library is opt-in behind its own extra.
- Encryption failures must fail fast. The library must never fall back
  to writing plaintext when encryption was requested.
- A nonce must never repeat under the same key. The library generates a
  fresh random nonce per encryption call itself, rather than trusting
  callers to manage that.
- Must still pass `report.passed_all_base()` for every backend, with
  encryption on and off.

## Decision

Add an `encryption` option to `ObjectStorageSaver.__init__` and
`from_conn_string`, default `None` (no encryption, zero behavior change).
A consumer who opts in supplies an object implementing a small
`KeyProvider` protocol:

```python
class KeyProvider(Protocol):
    def get_key(self, thread_id: str, key_id: str | None = None) -> tuple[str, bytes]: ...
```

On write, the saver calls `get_key(thread_id)` with `key_id=None` to ask
for the provider's current key, and gets back `(key_id, key_bytes)`. On
read, it calls `get_key(thread_id, key_id=...)` with the `key_id` recorded
in the object itself, so the provider can resolve the *exact* historical
key a given checkpoint was written under. That round trip is what makes
key rotation possible: without it, rotating a key would make every
checkpoint written under the old one unreadable.

`key_id` is an opaque string chosen entirely by the provider, and this ADR
imposes no format or length constraint on it. Rotation support is
likewise entirely the provider's concern: the protocol only requires
`get_key(thread_id, key_id=None)` to return *a* key. Whether a given
`KeyProvider` implementation can resolve arbitrary historical `key_id`s
(and thus supports rotation) is unspecified by the library. A
single-key provider that ignores `key_id` on every call is a valid,
minimal implementation.

Keys are scoped per-`thread_id` only, not per-`checkpoint_ns`. A
provider that wants finer-grained keying can fold `checkpoint_ns` into
its own key derivation, but the protocol doesn't pass it. That's a
deliberate YAGNI cut, not an oversight.

The library owns only the AEAD primitive (AES-256-GCM, via the
`cryptography` package, gated behind a new `encryption` extra). It never
talks to a KMS itself and never persists key material anywhere beyond the
lifetime of one encrypt/decrypt call. A `KeyProvider` implementation is
where a consumer wires up their actual KMS, Vault, or static-key setup.

### Wire format

This builds directly on [ADR 0003](0003-checkpoint-compression.md)'s
internal codec registry, specifically its `Codec` protocol and
`_wrap`/`_unwrap` shape. Encryption is a second, independent
`_wrap`/`_unwrap` pair, not a field bolted onto compression's dict.
It nests around whatever `_wrap` for compression already produced:

```python
inner = ormsgpack.packb({"checkpoint": [...], "metadata": [...], "parent_checkpoint_id": ...})
compressed = _wrap(inner, codec_name="zlib")  # ADR 0003's layer; a no-op if compression is off

key_id, key = key_provider.get_key(thread_id)
aad = "\x00".join([thread_id, checkpoint_ns, checkpoint_id]).encode()  # + task_id, str(idx) for writes
outer = _wrap_enc(compressed, key_id=key_id, key=key, aad=aad)
# ormsgpack.packb({"enc": "aes-256-gcm", "key_id": key_id, "nonce": ..., "payload": ciphertext})
```

`_wrap_enc` generates a fresh random nonce per call and encrypts
`compressed` with AES-256-GCM under `key`, passing the object's real
storage identity as AEAD associated data, not `thread_id` alone. For a
checkpoint that's `thread_id`, `checkpoint_ns`, and `checkpoint_id`,
mirroring `keys.checkpoint_key`'s path. For a write it's those three plus
`task_id` and `idx`, mirroring `keys.write_key`'s path -- two writes under
the *same* checkpoint still occupy distinct `(task_id, idx)` slots, so
without binding those too, one write's ciphertext could be relocated onto
another write's path within the same checkpoint undetected. Binding the
full identity means any move to a different thread, namespace,
checkpoint, task, or write index fails decryption instead of silently
decrypting under whatever key that path resolves to. Reading resolves the
key via `key_provider.get_key(thread_id, key_id=obj["key_id"])` and
decrypts with the same AAD to recover `compressed`; that result is then
checked for a `"codec"` key and decompressed if present, per ADR 0003.
Neither layer needs to know the other exists. Either can be a no-op
(object unencrypted, or uncompressed) independently, which is what makes
the `"none"`/`None` defaults from both ADRs compose correctly without a
combined "are both off" special case. When a `KeyProvider` is configured
on the saver, reading an object that was never encrypted is also treated
as an error, not a silent pass-through -- otherwise "encrypted at rest"
would only be a promise about new writes, not something enforced per
object.

AES-256-GCM's 96-bit random nonce has a birthday-bound collision risk
that becomes non-negligible well before it's exhausted. NIST guidance
puts the safe ceiling for a random-nonce 96-bit construction around 2^32
encryptions under one key. A long-lived thread on a static key could
approach that over a long enough operational lifetime. This ADR doesn't
add nonce-counting to the library; instead, `KeyProvider` implementers
should treat encryption volume, not just a calendar schedule, as a
rotation trigger for a given `key_id`.

The pipeline order is always compress, then encrypt, on write, and
decrypt, then decompress, on read. Encrypting before compressing would
defeat the compression step entirely, since ciphertext is high-entropy
and doesn't compress. That ordering is enforced by which `_wrap` call is
made first in `envelope.py`'s pack functions, not by anything either
layer knows about the other.

### Per-object encryption, like per-object codecs

Each object records its own `"enc"` and `"key_id"` fields, the same way
ADR 0003's objects record their own `"codec"`. A fleet can turn encryption
on for new writes, or rotate to a new key, without migrating existing
objects, as long as the `KeyProvider` can still resolve every `key_id`
that's ever been used.

### How it fits the existing architecture

`envelope.py` gains a `_wrap_enc`/`_unwrap_enc` pair alongside ADR 0003's
`_wrap`/`_unwrap`, following the same `Codec`-registry shape (one
registry entry for `"aes-256-gcm"`, room to add another algorithm later
without touching either pack/unpack function). `saver.py` threads the
`KeyProvider` through construction into the pack/unpack calls exactly the
way it threads the compression codec. `keys.py` is untouched.

`KeyProvider.get_key` stays sync-only in the protocol. A real
implementation (a KMS call, a Vault lookup) is blocking I/O, same as an
`fs` read or write. `saver.py`'s async core (`_aput`/`_aget_tuple`/etc.)
calls it via `asyncio.to_thread`, the same treatment it already gives its
other blocking I/O, rather than growing an `aget_key` variant on the
protocol.

### What changes for someone who opts in

- Object bytes are confidential at rest, independent of who else can read
  the bucket.
- AES-GCM's CPU cost is small, generally cheaper than the compression
  step it usually follows.
- Correctness of the whole scheme now depends on the consumer's
  `KeyProvider`: its availability, its ability to resolve old `key_id`s,
  and its own access control. The library can't verify any of that.
- Losing a `KeyProvider`'s ability to resolve a given `key_id` makes every
  object written under that key permanently unreadable. There's no
  recovery path inside this library.
- `list(filter=...)` is already O(n) (see README's documented non-goal
  on this). With encryption on, filtering by metadata means
  decrypting every candidate object, which means one `KeyProvider.get_key`
  call per candidate in the worst case. Most KMS `Decrypt`/
  `GenerateDataKey` APIs have per-account rate limits, so a large
  filtered `list()` call can get throttled, not just slowed down. A
  `KeyProvider`'s own caching of resolved keys by `key_id` stops being
  optional once `list()` is in the picture.

## Alternatives considered

### A. Direct KMS integration built into the library

Have the library talk to AWS KMS or GCP KMS directly to wrap and unwrap a
data encryption key, instead of delegating to a consumer-supplied
provider.

Rejected: it adds a cloud-SDK dependency and ties key-backend choice to
storage-backend choice in a way that doesn't generalize well. A
checkpoint stored in S3 might reasonably want a GCP KMS key, or vice
versa, and baking in one cloud's KMS client doesn't support that without
the library eventually growing several KMS integrations to maintain.

### B. User-supplied raw symmetric key only, no rotation

Skip `key_id` and the provider protocol; take one static key at
construction and use it for everything.

Rejected as the sole option: it works for the simplest case but has no
rotation story. Anyone who ever needs to rotate a key loses access to
everything encrypted under the old one, with no clean path forward. The
`KeyProvider` protocol still allows a trivial single-key implementation
for a consumer who genuinely doesn't need rotation, so nothing is lost by
generalizing to it instead.

### C. Rely on server-side encryption (S3 SSE, GCS default encryption) instead

Recommend consumers turn on their storage provider's built-in
encryption-at-rest and skip a client-side scheme entirely.

Rejected as sufficient on its own: server-side encryption protects
against storage-layer compromise, not against anyone who already has
bucket read access, which is exactly the shared-bucket scenario the
README's lifecycle-rule warning already flags. Client-side encryption is
complementary to server-side encryption, not a replacement for it, and
this ADR doesn't ask a consumer to choose one over the other.

## Consequences

**Positive**

- Confidentiality for PII or secrets in graph state, even on buckets with
  broader access than the application itself.
- Key rotation is supported through `key_id`, without a migration step.
- Zero behavior change for anyone who doesn't opt in.
- Fits inside the existing `envelope.py`/`saver.py` boundary the same way
  compression does.

**Negative / risks**

- Correctness rests entirely on the consumer's `KeyProvider`
  implementation: availability, key resolution, and its own access
  control are all outside this library's reach.
- An unavailable `KeyProvider` blocks reads and writes outright, a
  stronger failure mode than a codec issue, which can't really happen at
  read time.
- A new dependency (`cryptography`) to track, opt-in only.
- Encrypted objects are opaque, the same tradeoff compression makes, and
  the two compound if both are enabled.

## Applicability

**Worth enabling encryption for:**

- Graph state carrying PII, credentials, or other sensitive values.
- Shared buckets where read access is broader than the application
  itself.
- Deployments with a compliance requirement around data-at-rest that
  bucket-level encryption alone doesn't satisfy.

**Stay on `encryption=None` for:**

- Buckets already scoped tightly to one application, with no sensitive
  data in state.
- Deployments without the operational capacity yet to run a
  `KeyProvider` reliably. An unavailable provider blocks all I/O.

## Follow-up / open questions

None outstanding.

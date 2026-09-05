# ADR 0007: Thread export/import for backup and cross-backend migration

- **Status:** Accepted (implemented)
- **Date:** 2026-09-04
- **Owners:** ObjectStorageSaver maintainers

## Context

There's no supported way today to move a thread's checkpoint history
between backends (local to S3, S3 to GCS) or to back it up outside the
bucket it already lives in. This is a different, narrower gap than the
migration tool [ADR 0002](0002-slatedb.md) defers: that one is scoped to
converting a thread's storage engine (fsspec-backed history to SlateDB),
not moving fsspec-backed history between filesystem providers. Nothing
in either ADR addresses the gap this one fills, and nothing here depends
on SlateDB shipping.

`keys.py`'s layout already makes this tractable: every object for a
thread lives under a predictable prefix
(`root/thread_id/checkpoint_ns/...`), and that layout is identical
regardless of which fsspec-backed filesystem sits underneath it. Moving a
thread between backends is, structurally, just copying bytes from one
prefix to another.

"Backend" here means the fsspec filesystem `ObjectStorageSaver` is
pointed at: local disk, S3, or GCS. It does not mean other
`BaseCheckpointSaver` implementations. The archive this ADR produces is
raw `ObjectStorageSaver`-internal bytes: `keys.py`'s path layout plus
whatever `envelope.py` encoding wrote them (ormsgpack, optional
compression, optional AES-GCM encryption). A Postgres or SQLite saver has
no notion of that layout or encoding. This is not a migration path to or
from a different saver implementation; see Applicability.

## Decision

Add two public methods to `ObjectStorageSaver`:

```python
def export_thread(self, thread_id: str) -> bytes: ...
def import_thread(self, archive: bytes, *, dest_thread_id: str | None = None, overwrite: bool = False) -> None: ...
```

with `aexport_thread`/`aimport_thread` async variants following the
existing async-core/sync-wrapper pattern (the logic is written once,
sync methods wrap it via `asyncio.run`, same as `put`/`get_tuple` today).

`export_thread` takes no `checkpoint_ns`, matching `delete_thread`'s
existing contract: it always operates on the whole thread. LangGraph
subgraphs write checkpoints under non-default `checkpoint_ns` values
within the same `thread_id`, so a namespace-scoped export would silently
drop subgraph history unless the caller enumerated every namespace
themselves. `keys.thread_prefix(root, thread_id)`, the same prefix
`_delete_thread` already walks, covers every namespace under a thread in
a single `_find`. There's no need for a `checkpoint_ns` parameter to
deliver the "full thread history" this ADR promises.

`export_thread` walks that same `_find` call, reads each object's raw
bytes with `_cat`, and packs them into a tar archive keyed by their path
relative to `root`. It does not go through `envelope.py` at all: it
operates one level below compression or encryption, on whatever bytes are
actually stored, so it works unchanged regardless of what
[ADR 0003](0003-checkpoint-compression.md) or
[ADR 0004](0004-checkpoint-encryption.md) configuration produced them.

If `_find` returns no keys for the thread, `export_thread` raises
`KeyError(thread_id)` rather than returning an empty archive. This is a
deliberate departure from `get_tuple`/`list`/`delete_thread`'s convention
of treating a missing prefix as a silent empty result: those methods
exist to answer "does this exist" or "make sure this is gone," where a
no-op is the correct outcome for something absent. `export_thread` exists
to produce a reliable backup or migration artifact. A caller who passes a
typo'd or already-migrated `thread_id` and silently gets back a valid,
zero-entry archive is in trouble: `import_thread` would then
"successfully" import that archive as nothing. That's a false-success
footgun, and worth failing loudly against instead.

`import_thread` takes no `thread_id` parameter: the source thread_id is
read directly off the archive's own entry paths (every entry is relative
to `root` and starts with the thread_id segment `export_thread` wrote it
under), since one archive only ever contains one thread's data. It
extracts the tar and replays each entry through `_pipe`, rewriting that
thread_id prefix to `dest_thread_id` when one is given (default: the
thread_id read from the archive). Before writing
anything, it checks every destination key with `_exists`; if any already
exist and `overwrite` is `False`, it raises without writing a single
byte. This guards against a single caller stepping on existing data by
accident; it isn't a lock. The README already documents that concurrent
writers on the same `thread_id` can race with no ordering guarantee
beyond checkpoint_id sort, and that applies here too: a concurrent write
to the destination between the `_exists` check and `import_thread`'s own
`_pipe` calls can still land, the same as any other same-thread race in
this saver.

### New module: `archive.py`

Tar packing and unpacking only, the same single-responsibility split
`keys.py` and `envelope.py` already follow. `saver.py` orchestrates:
it calls `_find`/`_cat` to gather bytes, hands them to `archive.py` to
pack, and does the reverse for import, exactly the same shape as its
other business-logic methods.

`unpack` is the one place this module isn't purely mechanical: `archive`
bytes handed to `import_thread` come from wherever a caller got them --
a backup file, a network transfer, another team's export -- so `unpack`
treats every entry path as untrusted input. An entry whose path is
absolute or contains a `..` component is rejected with `ValueError`
before `saver.py` ever turns it into a destination key; without this,
an archive built with an entry like `"../../marker.txt"` would make
`import_thread` write outside `root` entirely, since destination keys
are built by plain string concatenation, not by anything that
constrains the result to stay under the store root. `unpack` also wraps
any `tarfile.TarError` (corrupt/non-tar bytes) into `ValueError`, so
`import_thread`'s documented `ValueError` contract (see below) covers
every way an `archive` argument can be malformed, not just the
multiple-thread-ids case.

### Input validation: thread_id can't contain `/`

`export_thread` rejects a `thread_id` containing `/`, and `import_thread`
rejects a `dest_thread_id` containing `/`, both with `ValueError`. The
archive's paths are relative to `root` and encode `thread_id` as their
first `/`-delimited segment (`keys.thread_id_from_relative_key`); a
`thread_id` that itself contains `/` makes that encoding ambiguous --
`import_thread` can't tell where the thread_id segment ends and the rest
of the path (`checkpoint_ns`, `checkpoints`, ...) begins, and silently
reconstructs the wrong destination path (only the first segment gets
rewritten under `dest_thread_id`, leaving the rest of the original
thread_id embedded where `checkpoint_ns` was expected). Rejecting at the
`export_thread`/`import_thread` boundary closes this for the archive
format specifically.

The same ambiguity was also live for ordinary `put`/`get_tuple`/`list`/
`put_writes`/`delete_thread` calls against a `thread_id` or
`checkpoint_ns` containing `/` -- a pre-existing gap discovered while
implementing this ADR, not introduced by it. That gap is closed too, in
`_thread_ns`/`_delete_thread` (`saver.py`), by the same "reject `/` with
`ValueError`" approach, rather than by changing the on-disk key format
itself (`keys.py`'s `/`-joined layout is unchanged for every `thread_id`/
`checkpoint_ns` that never contained `/`, which is every value that ever
worked correctly). That fix is orthogonal to this ADR's own scope --
it's a base-saver input-validation fix, not part of the export/import
feature -- documented here only because this ADR's review is what
surfaced it.

### Why raw bytes instead of re-serializing through the public API

`export_thread` copies bytes verbatim rather than reading each checkpoint
through `get_tuple`/`list` and re-encoding it into some independent
manifest format. Copying verbatim means an archive implicitly preserves
whatever envelope configuration produced it: a checkpoint written with
compression and encryption enabled stays compressed and encrypted after
import, with no re-encoding step to get wrong. See Alternative A for the
tradeoff this makes against a more storage-format-independent option.

### What changes for someone who uses it

- A thread's full history (checkpoints and writes) can move to a
  different backend, region, or bucket, or be written out for backup,
  without any custom scripting against `_find`/`_cat`/`_pipe` directly.
- An archive is only as portable as its contents allow: if the source
  checkpoints were encrypted (ADR 0004), the archive stays encrypted
  under that same key, and importing it somewhere the original
  `KeyProvider` can't resolve that `key_id` makes it permanently
  unreadable there. Export/import doesn't re-key anything.
- Renaming an encrypted thread via `dest_thread_id` permanently breaks
  decryption. AES-256-GCM's associated data is bound to `thread_id`
  (ADR 0004; see the README's Encryption section and
  `test_decrypt_fails_when_object_moved_to_a_different_thread` in
  `tests/unit/test_envelope.py`), so `import_thread` writing an encrypted
  object under a different `thread_id` than it was encrypted under
  guarantees `InvalidTag` on every future read of it, even with a
  perfectly working `KeyProvider`. `import_thread` doesn't detect this at
  import time: it stays opaque to `envelope.py`, the same way
  `export_thread` does (see "Why raw bytes instead of re-serializing
  through the public API"), so the import itself succeeds and the failure
  only surfaces later, on read. Only rename an encrypted thread's archive
  if you're also re-encrypting it under the destination `thread_id`
  yourself before import -- something this ADR doesn't provide.
- The whole archive is built and held in memory as `bytes`, so a very
  large thread's full history could be a real amount of memory to hold
  at once. See Follow-up.

## Alternatives considered

### A. Re-serialized manifest format

Walk the thread via the public `get_tuple`/`list` API, decode each
checkpoint and write, and produce a self-contained JSON or msgpack
manifest independent of internal key layout or envelope format.

This would be more portable across a future envelope format change,
since the manifest wouldn't depend on today's `envelope.py` internals at
all. Rejected for now because it duplicates logic `saver.py` and
`envelope.py` already have, costs a decode-then-re-encode pass over every
object, and buys portability the raw-bytes approach doesn't obviously
need: a raw-bytes archive already round-trips through whatever
compression/encryption configuration produced it, on both ends, without
having to represent that configuration in a separate manifest schema.

### B. CLI-only tool, no library method

Ship this only as a standalone script or console command, not as methods
on `ObjectStorageSaver`.

Rejected: a library method is directly usable from backup jobs, ops
scripts, and tests, not just interactively. A thin CLI wrapper around
`export_thread`/`import_thread` is a reasonable follow-up once the
methods exist, not a reason to withhold the methods themselves.

### C. Do nothing (status quo)

Rejected: this is a real, currently unaddressed gap. [ADR 0002](0002-slatedb.md)
flags a different migration gap (fsspec-backed history to SlateDB) as
needed eventually, but neither ADR addresses moving a thread between
fsspec filesystem providers, and nothing about this ADR depends on
SlateDB landing first.

## Consequences

**Positive**

- Fills a documented gap: no supported way exists today to move or back
  up a thread's history.
- No coupling to envelope format changes. Whatever ADR 0003/0004 do to
  the bytes, export/import doesn't need to know.
- No new dependency (`tarfile` is standard library).
- Testable against every existing backend combination, since the
  operation is just bytes in, bytes out.

**Negative / risks**

- Doesn't validate that an archive's contents will actually be usable at
  the destination. An encrypted archive imports fine as opaque bytes but
  can silently fail to decrypt later if the destination's `KeyProvider`
  can't resolve the key it needs -- or, if `dest_thread_id` renamed it,
  fails to decrypt unconditionally, regardless of `KeyProvider`
  correctness, because AES-256-GCM's associated data is bound to
  `thread_id` (see "What changes for someone who uses it").
- Buffers a full thread's history in memory on both export and import;
  see Follow-up on streaming.
- Two new public methods (times two for async) is real, permanent API
  surface to keep stable and covered by tests going forward.

## Applicability

**Worth using export/import for:**

- Backup jobs and disaster recovery for a specific thread.
- Moving a thread to a different bucket, region, or storage provider.
- Copying a thread's history for local debugging or reproduction without
  touching the original.

**Not a fit for:**

- Retention or garbage collection. Export/import moves and copies data;
  it never expires it, and doesn't reopen the GC non-goal the project
  already rules out.
- Bulk migration of an entire bucket. This ADR is scoped to one thread at
  a time.
- Migrating to or from a different `BaseCheckpointSaver` implementation
  (Postgres, SQLite, MongoDB, etc.). The archive is raw bytes in
  `ObjectStorageSaver`'s own key layout and envelope encoding. A different
  saver's storage format doesn't know how to read it, and nothing here
  produces or consumes any other saver's schema. "Backend" in this ADR's
  title means the fsspec filesystem underneath `ObjectStorageSaver`
  (local/S3/GCS), not the choice of saver class.

## Follow-up / open questions

- Whether export should support decrypting and re-encrypting under a
  different key at export or import time depends on
  [ADR 0004](0004-checkpoint-encryption.md) landing first, and isn't
  addressed here.
- A CLI wrapper around these methods (Alternative B) is a reasonable
  follow-up, not part of this ADR's scope.
- Streaming a very large thread's export/import instead of buffering the
  whole archive in memory is an accepted limitation for now, in the same
  spirit as `list(filter=...)`'s documented O(n) scan. Worth revisiting
  if real thread sizes make it a problem.
- README's Architecture decision records section links this ADR; a fuller
  usage section (mirroring how `compression`/`encryption` are documented)
  is still needed once this actually ships.

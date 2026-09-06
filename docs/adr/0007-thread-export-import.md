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
def import_thread(self, archive_bytes: bytes, *, dest_thread_id: str | None = None, overwrite: bool = False) -> None: ...
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

### Input validation: every identifier must be a safe path segment

`thread_id`, `checkpoint_id`, and `task_id` must each be a single,
non-empty path segment: not `.`, not `..`, and not containing `/`.
`checkpoint_ns` follows the same rule except `checkpoint_ns=""` (the
default namespace) is always valid, since `keys.py` already special-cases
an empty `checkpoint_ns` as "omit this segment" rather than joining it in
literally. `export_thread`'s `thread_id` and `import_thread`'s
`dest_thread_id` follow the same rule as `thread_id` above. All of these
raise `ValueError` via a single shared helper, `_check_safe_segment(label,
value, *, allow_empty=False)` (`saver.py`), called from `_thread_ns`,
`_delete_thread`, `_export_thread`, `_import_thread`, and the
`checkpoint_id`/`task_id` sites in `_put`/`_get_tuple`/`_put_writes`.

This rule, and how it was arrived at, is worth spelling out. It took
four review passes to get right, and each wrong intermediate version was
independently confirmed exploitable by direct execution:

1. **First pass:** reject `thread_id`/`dest_thread_id` containing `/`.
   The motivation was the archive format specifically. Its paths are
   relative to `root` and encode `thread_id` as their first `/`-delimited
   segment (`keys.thread_id_from_relative_key`), so a `thread_id`
   containing `/` makes that encoding ambiguous: `import_thread` can't
   tell where the thread_id segment ends and the rest of the path
   (`checkpoint_ns`, `checkpoints`, ...) begins, and silently
   reconstructs the wrong destination path. While implementing this, the
   same ambiguity turned out to already be live for ordinary
   `put`/`get_tuple`/`list`/`put_writes`/`delete_thread` calls against a
   `thread_id`/`checkpoint_ns` containing `/`. That's a pre-existing gap,
   not introduced by this ADR, and it was closed the same way: reject
   with `ValueError`, no on-disk format change (`keys.py`'s `/`-joined
   layout is unchanged for every value that never contained `/`).
2. **Second pass:** the first pass only checked `thread_id`/
   `checkpoint_ns`. `checkpoint_id` (`_put`, the explicit-id branch of
   `_get_tuple`, `_put_writes`) and `task_id` (`_put_writes`) were never
   checked at all, since they don't flow through `_thread_ns`. Confirmed
   by direct reproduction: a `checkpoint_id` of `"../../../../tmp/evil"`
   passed to plain `put` -- no archive or import involved -- wrote a file
   outside `root` entirely. This pass also folded the by-then-three
   hand-rolled `"/" in value` checks (one each in `_thread_ns`,
   `_export_thread`, `_import_thread`) into a single
   `_check_no_slash(label, value)` helper, and fixed two more bugs found
   alongside it, both in the archive path:
   - `keys.thread_id_from_relative_key` accepted a bare segment with no
     nested subpath (an archive entry literally named `"t1"` rather than
     `"t1/checkpoints/..."`) as a valid thread_id. That made
     `_import_thread` compute a destination path equal to the thread's
     own prefix and crash with an undocumented `IsADirectoryError`
     instead of the `ValueError` this ADR's contract promises. Fixed by
     requiring an actual `/` followed by a non-empty remainder.
   - `target_thread_id = dest_thread_id or source_thread_id` treated an
     explicitly-passed empty string the same as "not given" -- `or`
     doesn't distinguish `""` from `None` -- silently reimporting under
     the *source* thread_id instead of the empty string actually passed.
     Fixed by checking `dest_thread_id is not None` instead.
3. **Third pass:** the second pass's fix, and the "reject `/`" framing
   generally, was still wrong. A bare `".."` or `"."` value contains no
   `/` character of its own, since the surrounding key-building f-string
   already supplies the slashes on both sides of it, so `"/" in value`
   never catches it. Confirmed by direct reproduction of three distinct
   consequences:
   - `delete_thread("..")` recursively deleted everything next to
     `root`, not just the checkpoint store.
   - `put_writes` with `task_id=".."` escaped the intended
     per-checkpoint writes subdirectory, landing writes for different
     checkpoints in the same location.
   - The second pass's own `dest_thread_id is not None` fix made
     `dest_thread_id=""` a *reachable, accepted* value. But an empty
     segment collapses the surrounding `//` the same way `"."` does,
     silently landing an imported thread's checkpoints inside a
     *different*, unrelated, pre-existing thread whose name matched
     whatever path segment came after the empty one.

   `_check_no_slash` was replaced with `_check_safe_segment`, described
   above, and `archive.py`'s own `_is_safe_path` (used by `unpack`, see
   the previous section) was extended to also reject a bare `.` path
   component -- the identical gap, for archive entry paths.
4. **Fourth pass:** the third pass's blocklist (`/`, `.`, `..`) still
   missed `\`. On a local-disk deployment where the underlying OS treats
   backslash as a path separator (Windows -- not this project's supported
   platform, per the README's OS badge, but the local-disk backend runs
   on whatever OS Python is running on, and the fix costs nothing for any
   legitimate identifier), a `thread_id`/`checkpoint_id`/`task_id` like
   `"..\\..\\evil"` passed every check so far. On such a deployment it
   would write outside `root` the same way a literal `/`-based `..` does
   on Linux/macOS. Confirmed by direct execution:
   `_check_safe_segment("thread_id", "..\\..\\etc")` raised nothing, and
   `archive._is_safe_path` accepted an archive entry path built the same
   way.

   Rather than add `\` as a fourth blocklisted character -- the pattern
   by this point being "find one more dangerous value, add one more
   special case" -- this pass switched to an allowlist.
   `keys.is_safe_segment(value)` (new function in `keys.py`, the natural
   home for path-segment rules alongside `thread_id_from_relative_key`
   and friends) accepts only letters (any script; this does not restrict
   to ASCII), digits, and `. : - _`, and still separately checks that the
   value isn't `.` or `..` on its own -- an allowlist charset alone
   doesn't rule out a value made entirely of allowed characters that's
   still exactly `.` or `..`. `_check_safe_segment` (`saver.py`) and
   `archive.py`'s `_is_safe_path` both call it now, closing `/`, `\`, NUL
   bytes, and every other non-matching character in one place instead of
   continuing to enumerate them. Verified directly: values like
   `"evil\x00null"`, `"evil*star"`, and `"evil\nline"` -- none considered
   in any earlier pass -- are all rejected, while every character
   actually used by this project's own tests and examples (`"child:1"`,
   `"ckpt-1"`, `"task_id"`) still passes.

Two takeaways generalize past this specific bug. First: "reject a
character" and "reject a value" are different checks, and a
path-segment safety rule needs the latter -- `.` and `..` are dangerous
as whole segment *values*, not because of any character they contain.
Second, and broader: a blocklist only closes the gaps someone has
already thought of, while an allowlist closes everything not already
considered *safe* -- at the cost of being more restrictive than a
consumer's existing usage might expect. That's a real, deliberate
tradeoff, not a free upgrade. `checkpoint_ns` values like `"child:1"`
(this project's own subgraph-namespace convention) and UUID-shaped
`checkpoint_id`s both fit inside `. : - _` plus alphanumerics; an
identifier scheme relying on other punctuation (email addresses, `@`/`+`
in a slug, etc.) would need to change to use this saver.

### A related, narrower gap: Unicode normalization (documented, not fixed)

The fourth pass's allowlist review also surfaced (and left unaddressed)
a different kind of collision: two byte-distinct strings that *look*
identical because they use different Unicode normalization forms of the
same characters (a precomposed "é", U+00E9, vs. the same letter spelled
as "e" + a combining acute accent, U+0065 U+0301) both pass
`keys.is_safe_segment` -- neither contains a disallowed character -- but
some filesystems (notably macOS's HFS+/APFS) normalize filenames on
write, so two thread_ids that look the same to a human, and are
byte-distinct in Python, can resolve to the identical on-disk path.
Verified directly on such a filesystem: writing under one normalization
form and reading back via the other returns the same file.

This is deliberately left as a documented limitation (see the README's
Known limitations) rather than fixed by normalizing every identifier
(e.g. to NFC) before use, for two reasons. First, scope: it isn't
reproducible on any of this project's tested targets -- Linux local disk
(most Linux filesystems, including ext4, store the exact byte sequence
with no normalization) or S3/GCS (both are also byte-exact key stores,
no normalization at all) -- only a macOS-local-disk deployment is
affected. Second, cost: silently normalizing a caller's exact string
before storing it is a different kind of behavior change than rejecting
an unsafe value outright (this ADR's approach everywhere else) -- it
changes what identifier a caller's string actually maps to, and
normalization has its own edge cases (some combining-character sequences
don't round-trip through NFC/NFD cleanly). Given the narrow,
platform-specific reproduction and the real cost of the alternative,
documenting it matches how this project already handles other
OS/filesystem-level tradeoffs (see the sync/async event-loop warning and
the same-thread concurrent-writer race in the README) rather than
building around every one of them in code.

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
- Done: the README has both an Architecture decision records entry
  linking this ADR and a full Thread export and import section
  (mirroring how `compression`/`encryption` are documented), added once
  this shipped.

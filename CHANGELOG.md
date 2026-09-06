# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning is `MAJOR.MINOR.PATCH`; the patch number bumps automatically on
every merge to `main` (see the `release` job in `.github/workflows/ci.yml`,
which also updates this file).

## [Unreleased]

## [0.1.14] - 2026-09-06

### Added

- `export_thread`/`aexport_thread` and `import_thread`/`aimport_thread` on
  `ObjectStorageSaver`: pack a thread's full checkpoint/write history
  (every `checkpoint_ns`) into a portable tar archive for backup, or to
  move a thread between local disk, S3, and GCS. See
  [ADR 0007](docs/adr/0007-thread-export-import.md).

### Fixed

- `put`/`get_tuple`/`list`/`put_writes`/`delete_thread` (and their async
  variants), and `export_thread`/`import_thread`, now raise `ValueError`
  if `thread_id`, `checkpoint_id`, or `task_id` isn't a single path
  segment matching letters, digits, and `. : - _` (and isn't `.` or `..`
  on their own); `checkpoint_ns` follows the same rule except `""`, the
  default namespace, is always valid. This saver's key layout joins these
  values with `/` as a path separator, so a value outside that set could
  collide with, or (containing `/`/`\` or equal to `..`) write or
  recursively delete outside, a different thread/checkpoint/task's
  storage keys. Confirmed by direct reproduction across three earlier,
  narrower attempts at this same fix, each closing one concretely
  demonstrated gap the previous one missed: checking only for a literal
  `/` (missed `checkpoint_id`/`task_id` entirely, and missed that a bare
  `.`/`..` value needs no `/` of its own); checking `.`/`..` too (missed
  `\`, which is a path separator on Windows -- not this project's
  supported platform, per the OS badge above, but cheap to close anyway
  since it costs no legitimate identifier); landing on an allowlist
  instead of continuing to blocklist individual characters, closing the
  entire class (including characters no prior pass had considered, e.g.
  NUL bytes) in one shot. No on-disk format change: every value that was
  already alphanumeric plus `. : - _` is unaffected. See
  [ADR 0007](docs/adr/0007-thread-export-import.md) for the full
  round-by-round history, including the two rounds' worth of dead ends
  kept for the record.
- Documented (not fixed -- see ADR 0007) a related, narrower limitation
  surfaced by the same review: two visually-identical identifier values
  using different Unicode normalization forms can collide on a
  normalizing local filesystem (macOS's HFS+/APFS). Not reproducible on
  this project's tested targets (Linux local disk, S3, GCS), so this
  saver does no normalization of its own.

## [0.1.12] - 2026-09-05

### Changed

- Sync calls (`put`/`get_tuple`/`list`/`put_writes`/`delete_thread`/
  `delete_expired`) against local disk (or any other non-async-native
  fsspec filesystem) now reuse a lazily-created, per-instance persistent
  background event loop instead of building and tearing down a new one on
  every call. No API change. Measured against local disk: `put` ~348µs to
  ~175µs mean, a 50% reduction (~2,870 to ~5,720 ops/sec, +99%);
  `get_tuple` ~360µs to ~187µs mean, a 48% reduction (~2,780 to ~5,360
  ops/sec, +93%); `put_writes` ~326µs to ~153µs mean, a 53% reduction
  (~3,070 to ~6,560 ops/sec, +114%). S3/GCS unaffected (already using a
  persistent loop via `fsspec_sync`). See
  [ADR 0006](docs/adr/0006-persistent-event-loop.md) and the README's
  [Performance](#-performance) section.

## [0.1.11] - 2026-09-05

### Added

- `encryption` constructor param on `ObjectStorageSaver` (and
  `from_conn_string`): a consumer-supplied `KeyProvider` implementation
  encrypts each checkpoint/write object with AES-256-GCM before upload,
  and decrypts on read. `None` (default) is byte-identical to every prior
  release. The AEAD's associated data is bound to each object's full
  storage identity (`thread_id`/`checkpoint_ns`/`checkpoint_id`, plus
  `task_id`/index for writes), so an object moved to a different path
  fails to decrypt instead of silently decrypting under the wrong key.
  Each object records its own `key_id`, so key rotation and mixed
  encrypted/unencrypted objects in the same bucket both work with no
  migration step. Needs the new `encryption` extra. See
  [ADR 0004](docs/adr/0004-checkpoint-encryption.md) and the README's
  Encryption section.

## [0.1.10] - 2026-09-05

### Added

- `compression` constructor param on `ObjectStorageSaver` (and
  `from_conn_string`, as a keyword or a `?compression=...` connection-string
  query parameter): `"none"` (default), `"zlib"`, `"lzma"`, or `"zstd"`
  (needs the new `compression` extra). Each checkpoint/write object records
  its own codec, so a bucket can mix codecs across deploys with no
  migration step, and `"none"` output stays byte-identical to every prior
  release. See [ADR 0003](docs/adr/0003-checkpoint-compression.md) and the
  README's Compression section.

## [0.1.8] - 2026-09-04

### Added

- `ttl` constructor param and `delete_expired`/`adelete_expired` methods on
  `ObjectStorageSaver` for age-based checkpoint/write expiry. Local
  filesystem relies on calling `delete_expired()` yourself (cron, k8s
  CronJob, ...); S3/GCS can instead use a bucket lifecycle rule against the
  saver's `root` prefix, with no saver code involved. See the README's
  Checkpoint TTL section for setup and the shared-bucket lifecycle-rule
  caveat. Setting `ttl` also logs a one-time `WARNING` on construction,
  since the saver has no way to verify a lifecycle rule actually exists.

## [0.1.7] - 2026-08-30

### Changed

- No changelog entries were added for this release.

## [0.1.6] - 2026-08-30

### Changed

- No changelog entries were added for this release.

## [0.1.5] - 2026-08-16

### Changed

- No changelog entries were added for this release.

## [0.1.4] - 2026-08-16

### Changed

- No changelog entries were added for this release.

## [0.1.3] - 2026-08-16

### Added

- Usage examples for `pip`, `uv`, and `poetry` (`examples/`): a minimal
  local-filesystem quickstart for each, plus multi-session S3 (`uv`) and
  GCS (`poetry`) examples demonstrating sequential sessions with resume,
  and the same pattern run concurrently via the async API.

### Fixed

- The sync API (`put`, `get_tuple`, `list`, `put_writes`, `delete_thread`)
  could break on the second call against S3/GCS with `RuntimeError: Event
  loop is closed`. `asyncio.run()` opened a fresh event loop per call, but
  `s3fs`/`gcsfs` bind their session to whichever loop is running at first
  use. Fixed by routing sync calls through the filesystem's own persistent
  background loop, and giving every `ObjectStorageSaver` its own
  filesystem instance (`skip_instance_cache=True`) so separate savers can
  no longer share (and cross-contaminate) a session.

## [0.1.2] - 2026-08-16

### Fixed

- The automated release job's version tag never reached the remote:
  `git push --follow-tags` only pushes *annotated* tags, and the tag was
  created lightweight. Switched to an annotated tag pushed explicitly.

## [0.1.1] - 2026-08-16

### Added

- GitHub Actions CI (`.github/workflows/ci.yml`): lint, unit tests across
  Python 3.11/3.12/3.13, integration tests against docker-compose S3/GCS
  emulators, and an automated release job that bumps the patch version,
  tags, and publishes a GitHub Release with the built wheel/sdist attached.
- `LICENSE` (MIT).

## [0.1.0] - 2026-08-16

### Added

- Initial release: `ObjectStorageSaver`, a LangGraph `BaseCheckpointSaver`
  backed by local filesystem, Google Cloud Storage, or AWS S3 through a
  single fsspec-backed class, chosen by connection string.
- Full sync and async API (`get_tuple`/`aget_tuple`, `put`/`aput`,
  `list`/`alist`, `put_writes`/`aput_writes`, `delete_thread`/`adelete_thread`),
  validated against the official `langgraph-checkpoint-conformance` suite
  on all three backends.
- Runtime type checking on the public API via `typeguard`.
- `py.typed` static type coverage.
- `Makefile` (`lint`, `test`, `test-unit`, `test-integration`, `compose-up`/
  `compose-down`, `build`) and a `docker-compose.yaml` for local S3/GCS
  emulators (`moto-server`, `fake-gcs-server`).

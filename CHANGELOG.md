# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning is `MAJOR.MINOR.PATCH`; the patch number bumps automatically on
every merge to `main` (see the `release` job in `.github/workflows/ci.yml`,
which also updates this file).

## [Unreleased]

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

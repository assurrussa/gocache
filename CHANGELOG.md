# Changelog

## v0.1.0 - 2026-08-14

- Add the `rcu` whole-snapshot cache with lock-free reads.
- Add serialized synchronous, periodic, and coalesced event refreshes.
- Preserve the last successful snapshot when a loader fails.
- Add context-bound lifecycle, initial-load waiting, and background error
  handling.
- Add race-tested concurrency coverage, examples, and benchmarks.

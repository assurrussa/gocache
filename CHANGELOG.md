# Changelog

## Unreleased

- Add the public `arc` package: a fixed-capacity Adaptive Replacement Cache
  with TTL, jitter, optional metrics, and single/batch cache-aside loaders.
- Add the public `simple` package: an unbounded concurrent TTL map with
  optional metrics and cache-aside loading.
- Coalesce concurrent loads by comparable key through an internal generic
  coordinator; overlapping ARC batches no longer rely on string keys or the
  first missing key as a batch identity.
- Recover loader panics as sentinel errors, preserve successful zero/nil
  values, and prevent canceled or failed loads from publishing new values.
- Base the port on `goshared` commit
  `0212b091afd75a5aa4897616ff66817aebd78d89`; retain
  `github.com/hashicorp/golang-lru/arc/v2` at `v2.0.7` as the only new runtime
  dependency.

## v0.1.0 - 2026-08-14

- Add the `rcu` whole-snapshot cache with lock-free reads.
- Add serialized synchronous, periodic, and coalesced event refreshes.
- Preserve the last successful snapshot when a loader fails.
- Add context-bound lifecycle, initial-load waiting, and background error
  handling.
- Add race-tested concurrency coverage, examples, and benchmarks.

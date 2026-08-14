// Package rcu provides a read-copy-update-style whole-snapshot cache.
//
// Loaders build complete maps away from readers. A successful refresh makes
// the new map visible with one atomic pointer swap, while a failed refresh
// leaves the previous snapshot untouched. Reads are lock-free; refreshes are
// serialized and honor context cancellation.
package rcu

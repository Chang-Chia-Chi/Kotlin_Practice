package infra.snapshotcache.core

/**
 * Shell. P1 fills this in.
 *
 * It will hold all mutable state - generation table, current pointer, refcounts, leases,
 * consecutive-failure counter, shutting-down flag - behind a single monitor, and will
 * never call [infra.snapshotcache.spi.GenerationStore] while holding it (plan 2.5).
 */
internal class GenerationRegistry

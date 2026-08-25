package infra.snapshotcache.core

/**
 * Shell. P4 fills this in.
 *
 * It will sequence ACQUIRING -> BUILDING -> VERIFYING -> PUBLISHING -> GC (spec 4.1),
 * executing every storage call outside the registry lock.
 */
internal class RefreshCycle

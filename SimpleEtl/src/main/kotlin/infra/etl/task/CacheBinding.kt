package infra.etl.task

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.api.SnapshotCache

/**
 * What a task file's `cache:` name resolves to (spec 3.6, 7.3).
 *
 * **Two fields, not one.** A [SnapshotCache] serves many groups and `copyOut(group, spec)` takes
 * the group, so a name alone cannot identify both the instance and the group without silently
 * conflating two namespaces - the task file's vocabulary and the cache's.
 *
 * The host builds these; nothing in this module discovers a cache. That is the same arrangement
 * `datasources`, `transforms` and `hooks` already have, and for the same reason: spec 8.6 makes
 * wiring a host obligation, and a framework that reached for a `SnapshotCache` on its own would
 * need a container this module deliberately does not boot.
 *
 * This is the one type in `infra.etl` that names `infra.snapshotcache`. The ArchUnit rule
 * `only task may depend on the snapshot cache` forbids every other package from doing so; it does
 * **not** assert that `infra.etl.task` does depend on it, and needs no canary for that direction,
 * because this file failing to compile is a louder signal than any rule.
 */
data class CacheBinding(val cache: SnapshotCache, val group: GroupId)

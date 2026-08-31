package infra.snapshotcache.core

import infra.snapshotcache.api.GroupId
import infra.snapshotcache.spi.describe
import org.jboss.logging.Logger

internal val eventLog: Logger = Logger.getLogger("infra.snapshotcache.core.events")

/**
 * Fires one [infra.snapshotcache.api.CacheEvents] call and swallows whatever the sink
 * throws. Every `events.` call in `core` goes through here - the guard belongs at the
 * seam, not at the discretion of each call site.
 *
 * The sink is caller-supplied (a metrics binder, typically) and reporting is best-effort
 * by contract, so a throwing one must never break the path that fired it:
 * - on the refresh side it would skip the abort epilogue, leaving a zombie generation
 *   record and its file behind for the process lifetime;
 * - on the acquire side it would escape between the registry's refcount increment and the
 *   [infra.snapshotcache.spi.SnapshotHandle] that owns the matching release, leaking the
 *   lease permanently and eventually wedging refresh at the K guard.
 */
internal fun emit(group: GroupId, fire: () -> Unit) {
    try {
        fire()
    } catch (failure: Exception) {
        eventLog.warnf("CacheEvents sink of group %s threw and was ignored: %s", group, failure.describe())
    }
}

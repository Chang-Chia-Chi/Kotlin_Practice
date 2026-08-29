package infra.snapshotarchive

import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicLong

/*
 * Shared internals of the archive layer. `infra.snapshotcache.spi` has its own `ident` and
 * `literal`, but plan 3c fences this package off from `spi`, so the archive layer carries
 * its own pair. Carrying it ONCE is the point of this file: the fence explains why the
 * framework's copies cannot be imported, and explains nothing about why two files in this
 * package should each have their own.
 */

/**
 * Quotes a SQL identifier. Table and column names reach the SQL builders from caller config,
 * so a reserved word or a mixed-case name would otherwise be a parse error.
 */
internal fun ident(name: String): String = "\"${name.replace("\"", "\"\"")}\""

/** Escapes a value being interpolated into a single-quoted SQL literal. */
internal fun literal(value: String): String = value.replace("'", "''")

/** Named threads, so a stack dump during a stuck archive run says which pool is stuck. */
internal fun named(prefix: String): ThreadFactory {
    val counter = AtomicLong()
    return ThreadFactory { runnable -> Thread(runnable, "$prefix-${counter.incrementAndGet()}") }
}

/**
 * The object-store key for one archived file: the manifest's `uri_prefix` is
 * `<bucket>/snapshots/<group>/v<version>/` (spec 18.2) while [ObjectStore] addresses within a
 * bucket, so the bucket segment comes off.
 *
 * `ManifestDao` derives `uri_prefix` in one place so the layout is defined once; this is the
 * matching one place that reads it back. Three separate copies of the walk - the archiver's
 * upload, the purge's delete and the diff's download - would each have to be found and
 * changed together the day the layout moves, which is the Shotgun Surgery the single
 * derivation was meant to avoid.
 */
internal fun objectKey(entry: ManifestEntry, objectKey: String, bucket: String): String =
    entry.uriPrefix.removePrefix("$bucket/") + objectKey

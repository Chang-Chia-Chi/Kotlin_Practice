package infra.shuttle.testkit

import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.TargetRef
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Spec 7.1 in memory: one copy per key, the newest content; every `store` returns a fresh ref and
 * `verify` is true only while that ref is the current one at its key (I6). `failNextStore` makes the
 * next `store` throw before writing anything, one-shot, for the crash matrix.
 */
class InMemoryTarget(private val location: String = "bucket") : ObjectStoreTarget {
    data class Call(val method: String, val key: String?)
    private class Stored(val ref: TargetRef, val bytes: ByteArray, val metadata: Map<String, String>)

    private val objects = ConcurrentHashMap<String, Stored>()
    private val versions = AtomicLong()
    val calls: MutableList<Call> = Collections.synchronizedList(mutableListOf())
    @Volatile var failNextStore = false

    val keys: Set<String> get() = objects.keys
    fun bytes(key: String): ByteArray = objects.getValue(key).bytes
    fun metadata(key: String): Map<String, String> = objects.getValue(key).metadata

    override suspend fun store(key: String, file: Path, metadata: Map<String, String>): TargetRef {
        calls += Call("store", key)
        if (failNextStore) {
            failNextStore = false
            throw IOException("injected: store failed")
        }
        val bytes = Files.readAllBytes(file)
        val ref = TargetRef("memory", location, key, "v${versions.incrementAndGet()}", bytes.size.toLong())
        objects[key] = Stored(ref, bytes, metadata)
        return ref
    }

    override suspend fun verify(ref: TargetRef): Boolean {
        calls += Call("verify", ref.key)
        return objects[ref.key]?.ref == ref
    }

    override suspend fun probe() {
        calls += Call("probe", null)
    }
}

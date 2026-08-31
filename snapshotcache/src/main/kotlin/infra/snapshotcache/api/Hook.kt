package infra.snapshotcache.api

/**
 * Test-only interleaving points. Production runs [NoOpHooks], so these cost
 * one virtual call and nothing else.
 */
enum class Hook {
    /** acquire has read the current pointer; refcount++ has not happened yet. */
    AFTER_READ_CURRENT,
    BEFORE_POINTER_SWAP,

    /** Published; GC has not run yet. */
    AFTER_POINTER_SWAP,
    BEFORE_DETACH,
    AFTER_VERIFY,
}

/**
 * Called as control passes each [Hook]. Tests park on latches here to make an
 * interleaving deterministic instead of sleep-tuned.
 */
fun interface HookRunner {
    fun at(hook: Hook)
}

/** Production hook runner: does nothing. */
val NoOpHooks = HookRunner { }

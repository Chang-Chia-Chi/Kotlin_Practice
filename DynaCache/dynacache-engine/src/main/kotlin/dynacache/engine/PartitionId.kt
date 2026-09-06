package dynacache.engine

/**
 * Which partition a key belongs to: a fixed-count local hash bucket with one executor and its
 * own store. Placement across nodes is the ring's vnodes, a separate layer (ADR 0001).
 */
@JvmInline
value class PartitionId(val index: Int)

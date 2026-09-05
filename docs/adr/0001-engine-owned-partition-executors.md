# The command engine owns its partition executors; placement and execution are separate layers

The engine could have been a synchronous library that callers must serialize per partition,
with the executors in the cluster module. We chose the engine owning one JDK single-thread
executor per fixed local partition (`submit(command)` returns a future; batches run on the
partition's thread), so the single-writer rule C1 holds by construction and no caller can get
it wrong. Partitions are fixed-count local hash buckets; the ring's vnodes decide placement
across nodes and never the executor. Fusing the two would tie the executor count and the
validity of a batch to the node set.

Consequence: the engine module contains threads (JDK only, no I/O, still no dependencies), and
a batch needs its keys on the same coordinator and the same partition; hash tags give that.

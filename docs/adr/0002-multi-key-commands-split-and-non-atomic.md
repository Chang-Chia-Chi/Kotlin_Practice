# Multi-key commands split across partitions and are not atomic

Single-node Redis runs `MGET`, `MSET` and multi-key `DEL` atomically on one thread; Redis
Cluster rejects them across slots with `-CROSSSLOT`. DynaCache's clients see a single-node
Redis, so rejecting would break almost every plain `MGET`, and locking several partition
executors at once would defeat the single-writer design. We chose the third way: the engine
fans a multi-key command out to the partitions involved, joins the replies in argument order,
and makes no atomicity promise across partitions. `KEYS`, `SCAN`, `DBSIZE` and `FLUSHDB`
compose the same way, partition by partition.

Consequence: a concurrent writer can land between two partitions of one `MGET`. Atomicity
across keys is available only through a batch (MULTI/EXEC or EVAL) with hash tags. A test pins
the non-atomic behaviour so nobody "fixes" it into a global lock.

Considered and rejected: `-CROSSSLOT` (needs cluster-aware clients, which are on the
do-not-build list); ordered locking of all involved executors (a cross-partition lock in the
engine).

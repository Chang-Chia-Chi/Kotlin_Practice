# DynaCache P4 - Persistence: RDB, WAL, Chandy-Lamport (T31 to T37)

Companion to `../plan.md`. Spec: `../design-spec.md` sections 2.8, 3 (C9, C10, C14), 4 (I12),
5.4 (expired keys absent from snapshots), 6.8, 6.8b, 9.

**Goal:** a node restarts warm from RDB plus WAL and loses no acknowledged write; the cluster
takes a consistent snapshot under traffic and can be restored from it.

**Architecture:** the RDB codec, the WAL and the snapshot engine are `dynacache.engine.persist`
(the only engine package touching `java.nio.file`). The write-path hook (append before apply)
lives in the partition executors of `dynacache-cluster`, since that is where a write becomes
concurrent. Chandy-Lamport is a cluster protocol over the `Transport` seam.

---

### T31 - RDB codec

- **Goal:** spec 2.8 RDB format.
- **Deliverables:** streaming writer and reader for `[header][entry]*[checksum]` with entries
  `[key_len][key][type][dvv][ttl_abs][value_bytes]` for String, Hash, List and Sorted Set;
  DVV encoding shared with T21; expired entries skipped at write time against the injected
  clock; checksum verified at read.
- **Blocked by:** T07, T09, T21.
- **Fixed contracts:** spec 2.8 RDB paragraph; spec 5.4 (expired keys absent).
- **Acceptance:** `rdb_save_restore_roundtrip` (codec level, every type, TTLs and DVVs
  intact), `rdb_excludes_expired`, `rdb_bad_checksum_rejected`, `rdb_truncated_file_rejected`.
- **Model:** Opus. **Size:** medium.

### T32 - Snapshot engine

- **Goal:** spec 2.8 non-blocking snapshot; C9.
- **Deliverables:** per partition, on its executor, an immutable point-in-time view of the
  partition (copy-on-write or a persistent structure; free) handed to a writer that
  serializes off the executor; the snapshot is taken between commands, so no batch or script
  is half inside it; save on interval and on graceful shutdown; restore at startup before the
  node joins; file naming and atomic rename on completion.
- **Blocked by:** T14, T31.
- **Fixed contracts:** C9; spec 2.8 "must not block the command path".
- **Acceptance:** `rdb_concurrent_writes` (writes during the save, the file is a valid point in
  time), `C9_snapshot_never_contains_half_a_batch` (a `MULTI/EXEC` of ten keys racing a
  snapshot: all ten or none), `snapshot_restore_on_startup`, `snapshot_does_not_block_reads`
  (a read completes while the writer is stalled by a slow injected sink).
- **Model:** Fable. **Size:** medium.

### T33 - WAL writer and reader

- **Goal:** spec 2.8 WAL entry format and crash recovery of the file.
- **Deliverables:** append-only writer for `[crc32][length][seq][op][payload]`; reader that
  returns every complete entry, stops at the first CRC failure or torn tail and reports where;
  sequence numbers strictly increasing; `NEVER` fsync policy only (the others are T34).
- **Blocked by:** T01.
- **Fixed contracts:** spec 2.8 WAL entry format.
- **Acceptance:** `wal_write_read_roundtrip`, `wal_crash_recovery` (torn last entry skipped,
  all earlier entries returned), `wal_crc_detects_corruption` (a flipped byte stops the reader
  at that entry), `wal_seq_strictly_increasing`.
- **Model:** Opus. **Size:** small.

### T34 - Fsync policies and group commit

- **Goal:** spec 2.8 fsync policies and group commit.
- **Deliverables:** `ALWAYS`, `EVERY_SECOND` (driven by the injected clock and a tick), `NEVER`;
  group commit: concurrent appenders enqueue, one flusher writes the batch and fsyncs once,
  every appender's durability future completes together; the fsync call goes through a small
  sink interface so tests count it (the filesystem is a true boundary).
- **Blocked by:** T33.
- **Fixed contracts:** spec 2.8 fsync policies; plan 2.5.
- **Acceptance:** `wal_fsync_always_durable` (one fsync per append), `wal_fsync_every_second_batches`
  (fsync count far below append count), `wal_group_commit_amortizes` (100 concurrent appenders,
  fsync count far below 100, every appender completes), `wal_group_commit_preserves_seq_order`.
- **Model:** Fable. **Size:** medium.

### T35 - WAL in the write path, checkpoint, recovery

- **Goal:** C14 end to end.
- **Deliverables:** the partition executors append every mutation to the node's WAL and await
  durability per policy before applying to the engine and replying; checkpoint after each
  successful RDB save truncates entries at or below the snapshot's sequence number; startup
  recovery loads RDB then replays entries after the checkpoint; replay of a mutation is
  idempotent (absolute TTLs, DVV-carrying writes).
- **Blocked by:** T32, T34.
- **Fixed contracts:** C14; spec 2.8 recovery sequence.
- **Acceptance:** `wal_checkpoint_truncates`, `wal_full_recovery` (write, snapshot, write
  more, crash, restore from RDB plus WAL, every key present), `wal_replay_idempotent`,
  `C14_reply_only_after_durable_append` (a stalled sink delays the reply).
- **Model:** Fable. **Size:** medium.

### T36 - Chandy-Lamport distributed snapshots

- **Goal:** spec 2.8 Chandy-Lamport; C10; I12.
- **Deliverables:** `Marker` message; the initiator records local state (T32 snapshot) and
  sends markers on every outgoing channel; a receiver on its first marker records state,
  sends markers, and starts recording in-flight messages on its other incoming channels;
  a marker on channel C stops recording C; completion when every channel is closed; per-node
  state files plus per-channel message logs form the snapshot set; abort on timeout with no
  partial files left; a restore path that loads state and replays recorded channel messages.
- **Blocked by:** T22, T32.
- **Fixed contracts:** C10; I12; spec 2.8 steps 1 to 5 and the timeout.
- **Acceptance:** `chandy_lamport_consistent_cut` (traffic during the snapshot; for every
  recorded B with A before B, A is recorded), `chandy_lamport_restorable`,
  `chandy_lamport_timeout_aborts` (a node killed mid-snapshot; no files, no state change),
  `C10_marker_on_every_channel`, `I12_reads_after_restore_return_snapshot_time_values`.
- **Model:** Fable. **Size:** large.

### T37 - P4 acceptance

- **Goal:** spec 9 in full, on the P2 acceptance harness.
- **Deliverables:** the acceptance class extended with: kill all three nodes, restart from
  RDB plus WAL, data intact; trigger a Chandy-Lamport snapshot under Jedis traffic, keep
  writing, restore the cluster from the snapshot, reads return snapshot-time values; TTLs
  fire; the memory-pressure and W-TinyLFU line of spec 9 exercised with a small threshold.
- **Blocked by:** T30, T35, T36.
- **Fixed contracts:** spec 9.
- **Acceptance:** `P4_acceptance_success_signal`; every P1 to P4 test green in the same run.
- **Model:** Opus. **Size:** small.

---

## P4 Exit Criteria

All spec 6.8 and 6.8b tests green; the spec 9 demo runs as `P4_acceptance_success_signal`.

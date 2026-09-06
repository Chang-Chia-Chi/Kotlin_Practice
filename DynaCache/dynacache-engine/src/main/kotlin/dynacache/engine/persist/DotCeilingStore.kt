package dynacache.engine.persist

import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption.ATOMIC_MOVE
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING
import java.nio.file.StandardOpenOption.WRITE

/**
 * Where a node keeps its **reserved ceiling**: the highest counter of the dots (the cluster's
 * `(node, counter)` write events, spec 2.5) it has promised never to hand out again. The node's
 * counter reserves a block at a time by persisting the block's top before it hands out the
 * block's first dot, so every dot ever handed out is at or below the last ceiling on disk and a
 * restart that resumes above that ceiling reuses nothing (C2), at the cost of at most one
 * unused block per restart. Two adapters: [inFile] for a node with a data directory, [inMemory]
 * for one without, which forgets on restart exactly as it forgets everything else.
 */
interface DotCeilingStore {

    /** The last ceiling reserved here, 0 when none ever was. Fails rather than answers 0 for a ceiling it cannot read. */
    fun load(): Long

    /** Records [ceiling] durably before returning: from here on, [load] answers at least it, restart or not. */
    fun reserve(ceiling: Long)

    companion object {
        fun inMemory(): DotCeilingStore = object : DotCeilingStore {
            @Volatile private var ceiling = 0L
            override fun load(): Long = ceiling
            override fun reserve(ceiling: Long) { this.ceiling = ceiling }
        }

        /** The ceiling as decimal text in [file], rewritten whole: write, fsync, rename over the last one. */
        fun inFile(file: Path): DotCeilingStore = object : DotCeilingStore {
            private val temp = file.resolveSibling("${file.fileName}.tmp")

            override fun load(): Long = if (Files.exists(file)) Files.readString(file).trim().toLong() else 0L

            override fun reserve(ceiling: Long) {
                Files.createDirectories(file.toAbsolutePath().parent)
                FileChannel.open(temp, CREATE, WRITE, TRUNCATE_EXISTING).use { channel ->
                    channel.write(ByteBuffer.wrap(ceiling.toString().toByteArray(Charsets.US_ASCII)))
                    channel.force(true)
                }
                Files.move(temp, file, ATOMIC_MOVE, REPLACE_EXISTING)
            }
        }
    }
}

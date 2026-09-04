package infra.shuttle.sftp

import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.TargetRef
import infra.shuttle.core.keyLeavesTarget
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.withContext
import org.jboss.logging.Logger
import sftp.connector.client.Overwrite
import sftp.connector.client.SftpClient
import java.nio.file.Files
import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap

/**
 * Spec 7.3: a copy lands on a partner server as an upload to `<key>.part` and a rename over
 * `<key>`, so the name the partner watches never holds half a file and holds exactly one copy.
 *
 * The rename is the whole design. An upload writes straight to the path it is aimed at - the
 * connector says so - so a target directory somebody is polling would see a growing file under the
 * name it is waiting for, and would take it. Writing under a name nobody watches and moving it into
 * place afterwards costs one extra round trip and removes that entirely.
 *
 * [directory] is never created here: it is the partner's folder and its absence is a deployment
 * fault, which [probe] reports at start-up (spec 12.1). The folders a *key* names below it are
 * this adapter's own doing and are created on first use.
 */
class SftpTarget(
    private val client: SftpClient,
    private val directory: String,
    /**
     * What the connector's operations are called on. The connector already runs its own blocking
     * work on a dispatcher sized to its pool, so this does not bound anything - it decides whose
     * clock the connector's timeouts and backoffs run on, and they have to be a real one. A caller
     * on a scheduler that skips time - a `runTest`, and anything else that installs its own `Delay`
     * - would otherwise have every request time out before the socket could answer.
     */
    private val io: CoroutineDispatcher,
) : ObjectStoreTarget {

    private val root = directory.trimEnd('/')

    /**
     * The key folders already made, so a route storing into one folder pays the two round trips of
     * a `mkdir` on a folder that is already there once instead of per file.
     *
     * ponytail: the ceiling is a folder taken away after this target made it - every later store
     * into it then fails until the process restarts. Nothing removes folders on a partner server
     * without a person doing it, and that person is removing a folder files are being delivered
     * into; if it ever shows up, the upgrade is to forget the folder when an upload fails.
     */
    private val folders = ConcurrentHashMap.newKeySet<String>()

    /**
     * Spec 7.1: afterwards the object at [key] is the one just written, and there is one of it.
     *
     * Both halves replace, which is not the connector's default and is the whole of I6. The partial
     * name is this adapter's own and nothing else may write it, so whatever is there is a store of
     * this key that died before its rename - refusing it would leave the key jammed by its own
     * wreckage until somebody logged in and deleted a file. The rename replaces because the key is
     * a pure function of the object's name (spec 7.1), so a retry aims at the name its own earlier
     * attempt took, and refusing there would fail every retry of every file that ever landed.
     *
     * [metadata] is not stored: SFTP has nowhere to put it. Nothing reads it back either - `verify`
     * is a stat - so it is dropped with a DEBUG line rather than written into a sidecar file the
     * partner never asked for.
     */
    override suspend fun store(key: String, file: Path, metadata: Map<String, String>): TargetRef = withContext(io) {
        // rule 13 on the resolved key: pathOf concatenates and the server resolves, so a `..` segment
        // would write outside the partner's folder. The pipeline rejects such a key first (ticket 25);
        // this is the last place before the write, and the only one that knows the write is a path.
        require(!keyLeavesTarget(key)) { "$key leaves the target directory $root" }
        val size = Files.size(file)
        val remote = pathOf(key)
        val partial = "$remote$PARTIAL"
        makeFolderFor(key)
        if (metadata.isNotEmpty()) log.debugv("{0}: SFTP holds no metadata, so {1} keys are not stored with it", remote, metadata.size)
        client.upload(file, partial, Overwrite.REPLACE)
        // The partial as the server has just listed it, which is what lets a rename whose reply was
        // lost tell its own landed file at the key from the copy an earlier store left there
        // (connector D46: under REPLACE the key is expected to be occupied, so size alone decides
        // nothing and the modification time has to come along). Passing it costs the one stat the
        // connector would otherwise make for itself.
        client.rename(partial, remote, Overwrite.REPLACE, listed = client.stat(partial))
        val landed = checkNotNull(client.stat(remote)) { "$remote is not there after the rename that put it there" }
        check(landed.size == size) { "stored $remote has ${landed.size} bytes, expected $size" }
        TargetRef("sftp", root, key, landed.modifiedAt.toString(), size)
    }

    override suspend fun verify(ref: TargetRef): Boolean = withContext(io) {
        val landed = client.stat(pathOf(ref.key))
        landed != null && landed.size == ref.size && landed.modifiedAt.toString() == ref.ref
    }

    override suspend fun probe() = withContext(io) {
        val entry = client.stat(root)
            ?: throw IllegalStateException("target directory $root does not exist on the partner server; it is never created here")
        check(entry.isDirectory) { "$root is a file, and a target directory has to be a directory" }
    }

    private fun pathOf(key: String) = "$root/${key.trimStart('/')}"

    /**
     * Makes the folders the key names under [directory], and only those: a key with no folder in it
     * asks for nothing, because filling in [directory] itself would create the partner's folder that
     * [probe] exists to refuse.
     */
    private suspend fun makeFolderFor(key: String) {
        val folder = key.trimStart('/').substringBeforeLast('/', "")
        if (folder.isEmpty()) return
        val path = "$root/$folder"
        if (folders.add(path)) client.mkdir(path, parents = true)
    }

    private companion object {
        const val PARTIAL = ".part"
        val log: Logger = Logger.getLogger(SftpTarget::class.java)
    }
}

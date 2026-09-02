package sftp.connector.transport

import java.time.Instant

/**
 * What the server said about one path at the moment it was asked.
 *
 * A claim, not a fact, and the difference matters everywhere this type is used. The file may have
 * grown, been moved, or stopped existing between the listing and the download - on a directory
 * another system is uploading into, all three are ordinary rather than exceptional. So nothing
 * built on this may assume it is still true; the download checks the size it was promised, and a
 * file that has gone is a thing that happens rather than a thing that failed.
 */
data class RemoteFile(
    /** Absolute as far as the server is concerned, which is what every later call must quote back. */
    val path: String,
    val size: Long,
    val modifiedAt: Instant,
    val isDirectory: Boolean,
) {
    /** The last segment of [path], which is the name a downloaded copy is given locally. */
    val name: String get() = path.substringAfterLast('/')
}

/** Whether the server should go on reporting entries of a directory, or stop where it is. */
enum class Listing {
    CONTINUE,

    /**
     * Enough. The server is told to stop mid-directory and the remote handle is closed cleanly, so
     * a listing that is abandoned early costs neither the rest of the directory nor the session.
     */
    STOP,
}

package sftp.connector.client

import sftp.connector.config.Digest
import java.nio.file.Path

/**
 * A file that is on local disk now, complete, with the digest of the bytes that arrived.
 *
 * This is deliberately not the same concept as a remote file. A remote file is the server's claim
 * about something that may already have changed; this is a fact about a file the connector wrote
 * and counted. There is no such thing as a half-downloaded [LocalFile]: one exists only after the
 * bytes were written, the count was checked against the size the listing promised, and the file was
 * moved into place under its final name.
 *
 * The digest is computed while the bytes stream past, so it costs nothing extra, and it is where
 * the connector's contribution to integrity ends. Comparing it against an expected value is the
 * application's job, because only the application knows where an expected value comes from - a
 * sidecar file, a manifest, a row in a database. [digestAlgorithm] travels with the value so that
 * comparison cannot quietly be made against a digest of a different kind, which would fail forever
 * and look like corruption.
 */
data class LocalFile(
    val path: Path,
    /** Bytes written, which is also the size the listing said the remote file had. */
    val size: Long,
    /** Lower-case hex. */
    val digest: String,
    val digestAlgorithm: Digest,
)

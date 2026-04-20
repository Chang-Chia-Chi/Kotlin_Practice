package adapter

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.sftp.SFTPClient
import net.schmizz.sshj.xfer.FileSystemFile
import java.nio.file.Files
import java.nio.file.Path
import java.util.*
import kotlin.io.path.absolutePathString

private const val DEFAULT_BUF = 128 * 1024 // 128 KiB

class SftpAdapter(
    private val ssh: SSHClient,
) {
    suspend fun sftpUploadAtomic(
        local: Path,
        remotePath: String,
    ) = withContext(Dispatchers.IO) {
        require(Files.isReadable(local)) { "Local file not readable: $local" }

        ssh.newSFTPClient().use { sftp ->
            // Ensure parent directories exist
            val remoteDir = remotePath.substringBeforeLast('/', missingDelimiterValue = "")
            if (remoteDir.isNotEmpty()) {
                safeMkdirs(sftp, remoteDir)
            }

            // Upload to a temp name, then rename
            val tmp =
                buildString {
                    append(remotePath)
                    append(".part.")
                    append(
                        UUID
                            .randomUUID()
                            .toString()
                            .replace("-", "")
                            .take(8),
                    )
                }

            // Stream upload
            sftp.put(FileSystemFile(local.absolutePathString()), tmp)

            // Optionally fsync directory/file if your server supports it (many SFTP servers don't expose this).
            // SSHJ does not expose fsync; rename is atomic within same directory on standard SFTP servers.

            // Rename temp -> final
            sftp.rename(tmp, remotePath)
        }
    }

    private fun safeMkdirs(
        sftp: SFTPClient,
        dir: String,
    ) {
        // SSHJ's mkdirs throws if directory exists; do a safe check
        try {
            sftp.lstat(dir) // will throw if not exists
        } catch (_: Exception) {
            sftp.mkdirs(dir)
        }
    }
}

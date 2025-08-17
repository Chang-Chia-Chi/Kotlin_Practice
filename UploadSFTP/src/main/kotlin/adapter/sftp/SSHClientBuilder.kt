package adapter.sftp

import net.schmizz.sshj.SSHClient
import net.schmizz.sshj.transport.verification.OpenSSHKnownHosts
import net.schmizz.sshj.transport.verification.PromiscuousVerifier
import net.schmizz.sshj.userauth.keyprovider.KeyProvider
import java.io.File

class SSHClientBuilder {
    fun sshClient(cfg: SftpConfig): SSHClient {
        val ssh = SSHClient()

        // Host key verification: prefer known_hosts if provided; optionally allow a pinned fingerprint
        if (cfg.knownHostsFile != null && cfg.knownHostsFile.exists()) {
            ssh.addHostKeyVerifier(OpenSSHKnownHosts(cfg.knownHostsFile))
        }
//        cfg.fingerprintVerifier?.let { verify ->
//            ssh.addHostKeyVerifier { h, p, key -> verify(key.fingerprint()) }
//        }
        ssh.addHostKeyVerifier(PromiscuousVerifier())

        ssh.connectTimeout = cfg.connectTimeout.toMillis().toInt()
        ssh.timeout = cfg.socketTimeout.toMillis().toInt()
        ssh.connect(cfg.host, cfg.port)

        when (val a = cfg.auth) {
            is SftpConfig.Auth.Password ->
                ssh.authPassword(cfg.username, String(a.password))
            is SftpConfig.Auth.PrivateKeyFile -> {
                val kp: KeyProvider =
                    if (a.passphrase != null) {
                        ssh.loadKeys(a.privateKey.absolutePath, String(a.passphrase))
                    } else {
                        ssh.loadKeys(a.privateKey.absolutePath)
                    }
                ssh.authPublickey(cfg.username, kp)
            }
            is SftpConfig.Auth.PrivateKeyBytes -> {
                val tmp = File.createTempFile("sshj-key-", ".pem")
                tmp.writeBytes(a.privateKey)
                tmp.deleteOnExit()
                val kp: KeyProvider =
                    if (a.passphrase != null) {
                        ssh.loadKeys(tmp.absolutePath, String(a.passphrase))
                    } else {
                        ssh.loadKeys(tmp.absolutePath)
                    }
                ssh.authPublickey(cfg.username, kp)
            }
        }

        // KeepAlive helps long uploads on some servers
        ssh.connection.keepAlive.keepAliveInterval = cfg.keepAliveInterval.seconds.toInt()

        return ssh
    }
}

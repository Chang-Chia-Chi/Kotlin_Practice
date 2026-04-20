package adapter.sftp

import java.io.File
import java.time.Duration

data class SftpConfig(
    val host: String,
    val port: Int = 22,
    val username: String,
    val auth: Auth,
    val connectTimeout: Duration = Duration.ofSeconds(10),
    val socketTimeout: Duration = Duration.ofSeconds(30),
    val keepAliveInterval: Duration = Duration.ofSeconds(30),
    val knownHostsFile: File? = File(System.getProperty("user.home"), ".ssh/known_hosts"),
    val fingerprintVerifier: String? = null,
) {
    sealed interface Auth {
        data class Password(
            val password: CharArray,
        ) : Auth {
            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as Password

                return password.contentEquals(other.password)
            }

            override fun hashCode(): Int = password.contentHashCode()
        }

        data class PrivateKeyFile(
            val privateKey: File,
            val passphrase: CharArray? = null,
        ) : Auth {
            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as PrivateKeyFile

                if (privateKey != other.privateKey) return false
                if (passphrase != null) {
                    if (other.passphrase == null) return false
                    if (!passphrase.contentEquals(other.passphrase)) return false
                } else if (other.passphrase != null) {
                    return false
                }

                return true
            }

            override fun hashCode(): Int {
                var result = privateKey.hashCode()
                result = 31 * result + (passphrase?.contentHashCode() ?: 0)
                return result
            }
        }

        data class PrivateKeyBytes(
            val privateKey: ByteArray,
            val passphrase: CharArray? = null,
        ) : Auth {
            override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (javaClass != other?.javaClass) return false

                other as PrivateKeyBytes

                if (!privateKey.contentEquals(other.privateKey)) return false
                if (passphrase != null) {
                    if (other.passphrase == null) return false
                    if (!passphrase.contentEquals(other.passphrase)) return false
                } else if (other.passphrase != null) {
                    return false
                }

                return true
            }

            override fun hashCode(): Int {
                var result = privateKey.contentHashCode()
                result = 31 * result + (passphrase?.contentHashCode() ?: 0)
                return result
            }
        }
    }
}

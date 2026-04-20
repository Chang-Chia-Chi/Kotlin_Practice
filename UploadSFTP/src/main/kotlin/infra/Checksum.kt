package infra

import java.io.InputStream
import java.security.MessageDigest

object Checksum {
    fun hash(
        algo: String,
        input: InputStream,
    ): String {
        val md = MessageDigest.getInstance(algo)
        val buf = ByteArray(64 * 1024)
        while (true) {
            val n = input.read(buf)
            if (n <= 0) break
            md.update(buf, 0, n)
        }
        return md.digest().joinToString("") { "%02x".format(it) }
    }
}

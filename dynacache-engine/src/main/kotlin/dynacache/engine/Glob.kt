package dynacache.engine

/**
 * Redis's glob, the one `KEYS` and `SCAN MATCH` use (`stringmatchlen` in `util.c`), ported to
 * bytes so a binary key matches the same way a text one does: `*` any run, `?` one byte,
 * `[abc]` and `[a-z]` a class, `[^abc]` a negated class, and `\` escaping any of them.
 *
 * A faithful port rather than a translation to [Regex]: the two disagree on the edges (an
 * unclosed class, a reversed range, a trailing backslash) and Redis's answer is the one clients
 * were written against. Recursive on `*`, as Redis is.
 */
internal fun globMatches(pattern: ByteArray, string: ByteArray): Boolean = matches(pattern, 0, string, 0)

private fun matches(p: ByteArray, patternFrom: Int, s: ByteArray, stringFrom: Int): Boolean {
    var pi = patternFrom
    var si = stringFrom
    while (pi < p.size && si < s.size) {
        when (p[pi]) {
            STAR -> {
                while (pi + 1 < p.size && p[pi + 1] == STAR) pi++
                if (pi + 1 == p.size) return true
                while (si < s.size) {
                    if (matches(p, pi + 1, s, si)) return true
                    si++
                }
                return false
            }
            QUESTION -> si++
            OPEN -> {
                pi++
                val negated = pi < p.size && p[pi] == CARET
                if (negated) pi++
                var hit = false
                while (true) {
                    if (pi >= p.size) {
                        // No closing bracket: Redis backs up so the step below lands on the end.
                        pi--
                        break
                    } else if (p[pi] == ESCAPE && pi + 1 < p.size) {
                        pi++
                        if (p[pi] == s[si]) hit = true
                    } else if (p[pi] == CLOSE) {
                        break
                    } else if (pi + 2 < p.size && p[pi + 1] == DASH) {
                        val low = minOf(p[pi].toInt(), p[pi + 2].toInt())
                        val high = maxOf(p[pi].toInt(), p[pi + 2].toInt())
                        pi += 2
                        if (s[si].toInt() in low..high) hit = true
                    } else if (p[pi] == s[si]) {
                        hit = true
                    }
                    pi++
                }
                if (negated == hit) return false
                si++
            }
            ESCAPE -> {
                if (pi + 1 < p.size) pi++
                if (p[pi] != s[si]) return false
                si++
            }
            else -> {
                if (p[pi] != s[si]) return false
                si++
            }
        }
        pi++
        if (si == s.size) {
            // The string ran out: only trailing `*` may still be spent.
            while (pi < p.size && p[pi] == STAR) pi++
            break
        }
    }
    return pi == p.size && si == s.size
}

private const val STAR = '*'.code.toByte()
private const val QUESTION = '?'.code.toByte()
private const val OPEN = '['.code.toByte()
private const val CLOSE = ']'.code.toByte()
private const val CARET = '^'.code.toByte()
private const val DASH = '-'.code.toByte()
private const val ESCAPE = '\\'.code.toByte()

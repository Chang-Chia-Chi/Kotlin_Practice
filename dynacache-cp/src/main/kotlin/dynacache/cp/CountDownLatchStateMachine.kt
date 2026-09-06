package dynacache.cp

import dynacache.engine.Command
import dynacache.engine.Key
import dynacache.engine.Reply

/**
 * The CountDownLatch (CP spec 3.4): per `cp:latch:*` key, what is left to count down, mutated
 * only by applying committed entries in log order. A latch is a one-time barrier, so it is armed
 * only from zero: a latch nobody has set counts zero, and a latch that has run out may be armed
 * again, but one still counting down may not be moved under the parties waiting on it.
 */
class CountDownLatchStateMachine {

    private val latches = HashMap<Key, Int>()

    fun apply(command: Command.Cp.CountDownLatch): Reply {
        val count = latches[command.key] ?: 0
        return when (command) {
            is Command.Cp.LatchSet -> arm(command.key, count, command.count)
            is Command.Cp.LatchReset -> arm(command.key, count, command.count)
            is Command.Cp.LatchDown -> {
                val left = if (count == 0) 0 else count - 1
                latches[command.key] = left
                Reply.Integer(left.toLong())
            }
            is Command.Cp.LatchGet -> Reply.Integer(count.toLong())
        }
    }

    private fun arm(key: Key, count: Int, to: Int): Reply =
        if (count != 0) {
            Reply.Error("ERR", "latch $key is still counting down from $count")
        } else {
            latches[key] = to
            Reply.Simple("OK")
        }

    fun snapshot(): Map<Key, Int> = HashMap(latches)

    fun restore(state: Map<Key, Int>) {
        latches.clear()
        latches.putAll(state)
    }
}

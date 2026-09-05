package dynacache.engine

/**
 * A command a client asked the engine to run. Sealed and frozen as a root (plan 2.3); each
 * ticket adds the variants it implements. `Ping` is the whole language for now.
 */
sealed class Command {
    data object Ping : Command()
}

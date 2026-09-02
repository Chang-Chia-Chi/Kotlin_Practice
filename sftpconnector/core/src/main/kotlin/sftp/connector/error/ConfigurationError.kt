package sftp.connector.error

/**
 * Configuration that cannot start a connector.
 *
 * Thrown while the configuration block is being built, so an unreachable endpoint description
 * or an impossible timeout surfaces at assembly time rather than on the first connect attempt
 * an hour into a run. No amount of waiting or retrying cures it.
 */
class ConfigurationError(message: String) : RuntimeException(message)

package com.mapreduce.exporter

/**
 * Fatal exception thrown when query-exporter configuration is invalid.
 * Contains all accumulated validation errors in a single message.
 */
class StartupException(message: String) : RuntimeException(message)

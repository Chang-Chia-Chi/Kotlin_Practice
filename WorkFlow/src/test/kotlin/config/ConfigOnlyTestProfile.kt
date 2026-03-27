package com.workflow.config

import io.quarkus.test.junit.QuarkusTestProfile

/**
 * Test profile that excludes engine/worker beans requiring Jdbi.
 * Used by [FrameworkConfigDefaultsTest] which only needs the config subsystem.
 */
class ConfigOnlyTestProfile : QuarkusTestProfile {
    override fun getConfigOverrides(): Map<String, String> = mapOf(
        "quarkus.arc.exclude-types" to "com.workflow.engine.**,com.workflow.worker.**,com.workflow.queryexporter.**",
    )
}

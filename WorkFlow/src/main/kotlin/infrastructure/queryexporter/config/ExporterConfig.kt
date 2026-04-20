package com.workflow.infrastructure.queryexporter.config

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.InputStream

data class ExporterConfig(
    val queries: Map<String, QueryConfig>,
) {
    companion object {
        private val mapper: ObjectMapper =
            ObjectMapper(YAMLFactory())
                .registerModule(KotlinModule.Builder().build())
                .registerModule(JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

        fun load(input: InputStream): ExporterConfig = mapper.readValue(input, ExporterConfig::class.java)
    }
}

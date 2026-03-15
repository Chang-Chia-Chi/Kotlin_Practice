package com.mapreduce.config

import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.trace.Tracer
import io.quarkus.arc.DefaultBean
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces

/**
 * Fallback CDI producer for the OpenTelemetry [Tracer].
 *
 * Returns a no-op tracer via [GlobalOpenTelemetry] when no OTel SDK is configured.
 * If `quarkus-opentelemetry` is added to the project, its Tracer producer
 * takes precedence over this [DefaultBean].
 */
@ApplicationScoped
class TracerProducer {

    @Produces
    @DefaultBean
    @ApplicationScoped
    fun tracer(): Tracer = GlobalOpenTelemetry.getTracer("mapreduce-framework")
}

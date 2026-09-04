package infra.shuttle.quarkus

import infra.shuttle.core.ObjectStoreTarget
import infra.shuttle.core.StateStore
import infra.shuttle.jdbi.JdbiStateStore
import io.micrometer.core.instrument.MeterRegistry
import io.quarkus.agroal.runtime.AgroalDataSourceUtil
import io.quarkus.arc.Arc
import io.quarkus.arc.InjectableInstance
import io.quarkus.runtime.ShutdownEvent
import io.quarkus.runtime.StartupEvent
import jakarta.enterprise.event.Observes
import jakarta.enterprise.inject.Any
import jakarta.enterprise.inject.Instance
import jakarta.enterprise.inject.Produces
import jakarta.inject.Singleton
import org.eclipse.microprofile.config.ConfigProvider
import org.eclipse.microprofile.config.inject.ConfigProperty
import org.eclipse.microprofile.health.HealthCheck
import org.eclipse.microprofile.health.HealthCheckResponse
import org.eclipse.microprofile.health.Readiness
import org.jboss.logging.Logger
import org.jdbi.v3.core.Jdbi
import java.nio.file.Path
import java.time.Clock

/** Rule 15 and spec 12.1 step 5 through the container: a name resolves to whatever CDI bean carries it. */
fun cdiBeans() = NamedBeans { name -> Arc.container().instance<kotlin.Any>(name).let { if (it.isAvailable) it.get() else null } }

/**
 * What a `${VAR}` reference resolves from (spec 13.1): the process environment, plus every upper-case name
 * MicroProfile Config knows, so a deployment may hand a secret over as an environment variable or through a
 * mounted properties file, and a test through a config override. Rule 25 is unchanged: the YAML holds references.
 */
fun environment(): Map<String, String> {
    val config = ConfigProvider.getConfig()
    val named = config.propertyNames.filter { VARIABLE.matches(it) }.associateWith { config.getOptionalValue(it, String::class.java).orElse(null) }
    return System.getenv() + named.filterValues { it != null }.mapValues { it.value!! }
}

private val VARIABLE = Regex("[A-Z][A-Z0-9_]*")

/**
 * The host's life inside Quarkus: built and started on the startup event
 * unless the process was launched in a command mode, closed on the shutdown event. The Quarkus datasource named
 * by `shuttleStateStore.oracle.datasource` becomes the JDBI store; a `StateStore` bean, when one exists, replaces
 * it (the test kit's in-memory store, through a test-tree producer), a `StoreReads` bean its read side, and an
 * `ObjectStoreTarget` bean named after a store replaces that store's target adapter. Nothing else is swapped.
 */
@Singleton
class ShuttleLifecycle(
    @ConfigProperty(name = "shuttle.config") private val files: List<String>,
    @ConfigProperty(name = "shuttle.mode", defaultValue = "serve") private val mode: String,
    private val registry: MeterRegistry,
    private val clock: Clock,
    private val stores: Instance<StateStore>,
    private val reads: Instance<StoreReads>,
    @Any private val targets: InjectableInstance<ObjectStoreTarget>,
) {
    // `final`: all-open opens every @Singleton class for CDI, and Kotlin forbids a private setter on an open property.
    @Volatile
    final var host: ShuttleHost? = null
        private set

    fun ready(): Boolean = host?.ready() ?: false

    fun onStart(@Observes event: StartupEvent) {
        if (mode != "serve") return
        val env = environment()
        val beans = cdiBeans()
        val config = ShuttleHost.load(files.map { Path.of(it.trim()) }, env, beans)
        val io = ShuttleHost.ioDispatcher(config)
        val (store, storeReads) = if (stores.isResolvable) {
            stores.get() to checkNotNull(reads.takeIf { it.isResolvable }?.get()) { "a StateStore bean needs a StoreReads bean beside it" }
        } else {
            val name = checkNotNull(config.stateStore?.datasource) { "shuttleStateStore.oracle.datasource is not set" }
            val jdbi = Jdbi.create(AgroalDataSourceUtil.dataSourceInstance(name).get())
            JdbiStateStore(jdbi, io, clock).let { it to StoreReads(it::transfers, it::outbox) }
        }
        val overrides = targets.handles().mapNotNull { h -> h.bean.name?.let { it to h.get() } }.toMap()
        host = ShuttleHost(config, env::get, beans, store, storeReads, registry, clock, targets = overrides, io = io).also { it.start() }
    }

    /** Spec 12.3 from the shutdown event; the datasource is Quarkus's and closes after every observer has run. */
    fun onStop(@Observes event: ShutdownEvent) {
        host?.let { runCatching { it.close() }.onFailure { e -> log.warn("shutdown failed", e) } }
        host = null
    }

    private companion object {
        val log: Logger = Logger.getLogger(ShuttleLifecycle::class.java)
    }
}

/** The one clock the module reads, born here and nowhere else (ArchitectureTest); a test constructs what it tests with its own. */
@Singleton
class ShuttleClock {
    @Produces
    @Singleton
    fun clock(): Clock = Clock.systemUTC()
}

/** Spec 10 at `/q/health/ready`: the supervisor's answer under the configured rule; DOWN before start and from the first moment of shutdown. */
@Readiness
@Singleton
class ShuttleReadiness(private val lifecycle: ShuttleLifecycle) : HealthCheck {
    override fun call(): HealthCheckResponse = HealthCheckResponse.named("shuttle-routes").status(lifecycle.ready()).build()
}

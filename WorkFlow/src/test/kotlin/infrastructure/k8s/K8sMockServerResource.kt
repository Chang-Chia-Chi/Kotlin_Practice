package com.workflow.infrastructure.k8s

import io.fabric8.kubernetes.client.server.mock.KubernetesCrudDispatcher
import io.fabric8.kubernetes.client.server.mock.KubernetesMockServer
import io.fabric8.mockwebserver.Context
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager
import okhttp3.mockwebserver.MockWebServer

/**
 * Boots a Fabric8 [KubernetesMockServer] in CRUD mode and rewires the Quarkus
 * kubernetes-client to point at it. CRUD mode is essential so that resources
 * created via any client (test or production) automatically deliver Watch events
 * to active watchers — that is what allows [com.workflow.worker.adapter.trigger.K8sJobTriggerDriver]
 * to observe Job completion in the E2E test.
 *
 * NOTE: Not currently referenced by any active test. Reserved for future K8s-trigger integration
 * tests once [com.workflow.worker.adapter.trigger.K8sJobTriggerDriver] is exercised via E2E.
 */
class K8sMockServerResource : QuarkusTestResourceLifecycleManager {

    companion object {
        @Volatile
        var server: KubernetesMockServer? = null
            private set
    }

    override fun start(): Map<String, String> {
        // 6.13.4 default constructors do not enable CRUD mode — we must pass a
        // KubernetesCrudDispatcher explicitly via the (Context, MockWebServer, Map, Dispatcher, boolean)
        // constructor.
        val s = KubernetesMockServer(
            Context(),
            MockWebServer(),
            HashMap(),
            KubernetesCrudDispatcher(),
            false, // useHttps
        )
        s.init()
        server = s
        return mapOf(
            "quarkus.kubernetes-client.api-server-url" to s.url(""),
            "quarkus.kubernetes-client.namespace" to "test-ns",
            "quarkus.kubernetes-client.trust-certs" to "true",
        )
    }

    override fun stop() {
        server?.destroy()
        server = null
    }
}

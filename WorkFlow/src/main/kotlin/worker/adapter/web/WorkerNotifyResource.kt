package com.workflow.worker.adapter.web

import com.workflow.worker.usecase.port.outbound.notification.WorkerNotifier
import jakarta.ws.rs.DefaultValue
import jakarta.ws.rs.POST
import jakarta.ws.rs.Path
import jakarta.ws.rs.QueryParam

/**
 * Internal HTTP endpoint for cross-pod worker notification.
 *
 * Called by [com.workflow.worker.adapter.http.HttpWorkerNotifier.signal] on remote peers to wake
 * local workers. No authentication -- relies on Kubernetes
 * NetworkPolicy for pod-to-pod traffic isolation. The endpoint is
 * idempotent: worst case is a spurious wake-up that finds no work.
 */
@Path("/internal/dispatch-notify")
class WorkerNotifyResource(
    private val notifier: WorkerNotifier,
) {
    @POST
    fun notify(@QueryParam("queue") @DefaultValue("default") queue: String) {
        notifier.onRemoteSignal(queue)
    }
}

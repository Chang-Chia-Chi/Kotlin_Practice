package com.workflow.leader

import jakarta.inject.Singleton

/**
 * Strategy for detecting whether the process is running inside Kubernetes.
 *
 * Production uses env-var detection. Tests inject a lambda.
 */
fun interface KubernetesDetector {
    fun isRunningInKubernetes(): Boolean
}

/** Default detector: checks KUBERNETES_SERVICE_HOST env var. */
@Singleton
class EnvKubernetesDetector : KubernetesDetector {
    override fun isRunningInKubernetes(): Boolean =
        System.getenv("KUBERNETES_SERVICE_HOST") != null
}

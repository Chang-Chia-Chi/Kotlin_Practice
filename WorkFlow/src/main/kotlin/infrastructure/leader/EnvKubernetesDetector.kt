package com.workflow.infrastructure.leader

import jakarta.inject.Singleton

@Singleton
class EnvKubernetesDetector : KubernetesDetector {
    override fun isRunningInKubernetes(): Boolean =
        System.getenv("KUBERNETES_SERVICE_HOST") != null
}

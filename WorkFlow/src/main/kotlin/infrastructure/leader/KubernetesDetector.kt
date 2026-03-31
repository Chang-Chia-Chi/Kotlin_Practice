package com.workflow.infrastructure.leader

fun interface KubernetesDetector {
    fun isRunningInKubernetes(): Boolean
}

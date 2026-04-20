package com.workflow.infrastructure.leader

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class KubernetesDetectorTest {

    // -- Functional interface contract ----------------------------------------

    @Test
    fun `KubernetesDetector lambda returning true indicates K8s environment`() {
        val detector = KubernetesDetector { true }
        assertTrue(detector.isRunningInKubernetes())
    }

    @Test
    fun `KubernetesDetector lambda returning false indicates non-K8s environment`() {
        val detector = KubernetesDetector { false }
        assertFalse(detector.isRunningInKubernetes())
    }

    // -- EnvKubernetesDetector ------------------------------------------------

    @Test
    fun `EnvKubernetesDetector returns false when KUBERNETES_SERVICE_HOST is not set`() {
        // In a standard test environment, KUBERNETES_SERVICE_HOST is not set.
        // If this test runs inside K8s, the env var will be present and the
        // assertion should be adjusted — but CI and local dev are not in K8s.
        val detector = EnvKubernetesDetector()
        assertFalse(
            detector.isRunningInKubernetes(),
            "Expected false in non-Kubernetes test environment"
        )
    }

    @Test
    fun `EnvKubernetesDetector implements KubernetesDetector interface`() {
        val detector: KubernetesDetector = EnvKubernetesDetector()
        // Should not throw — just verify the interface contract is satisfied
        detector.isRunningInKubernetes()
    }
}

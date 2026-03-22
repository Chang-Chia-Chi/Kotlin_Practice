package com.workflow.leader

import kotlin.test.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Tests for [KubernetesDetector] functional interface and [EnvKubernetesDetector].
 *
 * Note on [EnvKubernetesDetector]: It reads `System.getenv("KUBERNETES_SERVICE_HOST")`,
 * which is set by the Kubernetes runtime. In a test environment (not running in K8s),
 * this env var is absent, so `isRunningInKubernetes()` returns false. We cannot
 * reliably set env vars in a unit test without JVM tricks (e.g., reflection on
 * ProcessEnvironment), which is fragile across JVM versions. Instead, we:
 *   1. Test the actual [EnvKubernetesDetector] in the local (non-K8s) environment.
 *   2. Test the functional interface contract with explicit true/false lambdas.
 */
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

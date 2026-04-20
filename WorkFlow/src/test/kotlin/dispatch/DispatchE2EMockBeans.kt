package com.workflow.dispatch

import com.workflow.dispatch.usecase.port.outbound.persistence.BaselineProvider
import com.workflow.dispatch.usecase.port.outbound.persistence.CandidateRepository
import com.workflow.dispatch.usecase.port.outbound.persistence.DispatchConfigRepository
import jakarta.enterprise.context.ApplicationScoped
import jakarta.enterprise.inject.Produces
import org.mockito.Mockito

/**
 * CDI producers for the three dispatch repositories that have no production CDI
 * registration (they are wired manually in the prod app from external systems).
 *
 * Without these, Quarkus deployment fails for [DispatchE2EHappyPathTest] because
 * `DispatchSimulationHandler` and `DispatchScatterHandler` require all three.
 *
 * The test class uses `@InjectMock` to *replace* these producer-supplied mocks
 * with per-test mocks programmed in `setupMocks()`. The producers exist purely
 * to satisfy CDI's deployment-time validation.
 */
@ApplicationScoped
class DispatchE2EMockBeans {

    @Produces
    @ApplicationScoped
    fun dispatchConfigRepository(): DispatchConfigRepository =
        Mockito.mock(DispatchConfigRepository::class.java)

    @Produces
    @ApplicationScoped
    fun candidateRepository(): CandidateRepository =
        Mockito.mock(CandidateRepository::class.java)

    @Produces
    @ApplicationScoped
    fun baselineProvider(): BaselineProvider =
        Mockito.mock(BaselineProvider::class.java)

}

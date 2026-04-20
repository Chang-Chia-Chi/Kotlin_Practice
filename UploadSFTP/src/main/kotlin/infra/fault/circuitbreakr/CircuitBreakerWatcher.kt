package infra.fault.circuitbreakr

import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.event.CircuitBreakerOnStateTransitionEvent
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.stateIn
import kotlinx.coroutines.launch

class BreakerWatcherFlow(
    breakers: List<CircuitBreaker>,
    scope: CoroutineScope,
) {
    val paused: StateFlow<Boolean> =
        breakers
            .statesFlow()
            .stateIn(scope, SharingStarted.Lazily, breakers.anyOpen())

    private fun List<CircuitBreaker>.statesFlow(): Flow<Boolean> =
        map { breaker ->
            channelFlow {
                send(breaker.state)
                breaker.eventPublisher.onStateTransition { event ->
                    trySend(event.stateTransition.toState)
                }
            }
        }.let { flows ->
            combine(flows) { states ->
                states.any { it == CircuitBreaker.State.OPEN }
            }
        }

    private fun List<CircuitBreaker>.anyOpen() = any { it.state == CircuitBreaker.State.OPEN }
}

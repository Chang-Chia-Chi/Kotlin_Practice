package infra.statistic

import kotlinx.atomicfu.atomic
import kotlinx.atomicfu.updateAndGet

class Ewma(
    private val alpha: Double = 0.2,
    init: Double = 1.0,
) {
    private val value = atomic(init)

    fun update(x: Double): Double = value.updateAndGet { prev -> prev + alpha * (x - prev) }

    fun value(): Double = value.value
}

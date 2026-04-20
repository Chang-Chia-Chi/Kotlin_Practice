package infra.statistic

fun interface CostEstimator<in T> {
    suspend fun estimate(value: T): Double
}

fun <T> fromEwma(
    ewma: Ewma,
    scale: Double = 1.0,
    min: Double = 0.0,
    max: Double = Double.POSITIVE_INFINITY,
): CostEstimator<T> =
    CostEstimator {
        (ewma.value() * scale).coerceIn(min, max)
    }

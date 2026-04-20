package extension

import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.*

@OptIn(ExperimentalCoroutinesApi::class)
fun <T, R> Flow<T>.unorderedMapAsync(
    concurrency: Int = 4,
    block: suspend (T) -> R,
): Flow<R> =
    flatMapMerge(concurrency) { value ->
        flow { emit(block(value)) }
    }

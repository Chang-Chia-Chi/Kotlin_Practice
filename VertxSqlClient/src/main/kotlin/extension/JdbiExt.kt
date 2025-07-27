package extension

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import org.jdbi.v3.core.HandleCallback
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.result.ResultIterable

fun <R, X : Exception> Jdbi.withHandleFlow(callback: HandleCallback<ResultIterable<R>, X>): Flow<R> =
    channelFlow {
        open().use { handle ->
            callback.withHandle(handle).iterator().forEach { send(it) }
        }
    }

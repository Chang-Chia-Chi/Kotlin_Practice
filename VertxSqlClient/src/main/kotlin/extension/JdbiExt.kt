package extension

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import org.jdbi.v3.core.HandleCallback
import org.jdbi.v3.core.Jdbi
import org.jdbi.v3.core.result.ResultIterable

fun <R, X : Exception> Jdbi.withHandleFlow(callback: HandleCallback<ResultIterable<R>, X>): Flow<R> =
    flow {
        open().use { handle ->
            callback.withHandle(handle).iterator().use { iter ->
                while (iter.hasNext()) {
                    emit(iter.next())
                }
            }
        }
    }

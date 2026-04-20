package jms

class FlowImpl<out T> : JmsListenerContextManager.Flow<T> {
    override suspend fun collect(collector: JmsListenerContextManager.FlowCollector<T>) {
        TODO("Not yet implemented")
    }
}

class FlowCollector<out T> : JmsListenerContextManager.FlowCollector<T> {
    override suspend fun emit(value: T) {
        TODO("Not yet implemented")
    }
}

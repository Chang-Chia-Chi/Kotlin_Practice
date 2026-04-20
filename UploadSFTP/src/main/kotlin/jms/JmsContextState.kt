package jms

enum class JmsContextState {
    CONNECTED,
    CONNECTING,
    DISCONNECTED,
    CONNECTION_DIED,
    NULL_CONNECTION,
}

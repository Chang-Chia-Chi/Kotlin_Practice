package com.workflow.worker.usecase.port.outbound.peer

interface PeerDiscovery {
    fun peers(): List<String>
}

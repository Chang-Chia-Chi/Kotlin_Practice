package com.workflow.dispatch.usecase.port.outbound.storage

interface StorageGateway {
    suspend fun uploadCsv(path: String, content: ByteArray)
    suspend fun uploadParquet(path: String, content: ByteArray)
}

package com.workflow.dispatch.usecase.port.outbound.storage

import java.io.File

interface StorageGateway {
    suspend fun uploadCsv(path: String, file: File)
    suspend fun uploadParquet(path: String, content: ByteArray)
}

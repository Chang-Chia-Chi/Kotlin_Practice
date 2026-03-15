package com.mapreduce.chain.api.dto

import com.mapreduce.chain.model.ChainJob

data class ChainResponse(
    val chainId: String,
    val chainType: String,
    val status: String,
    val currentStep: Int,
    val totalSteps: Int,
    val failurePolicy: String,
    val lastStepOutput: String?,
    val errorMessage: String?,
    val createdAt: String?,
    val updatedAt: String?,
) {
    companion object {
        fun from(chain: ChainJob) = ChainResponse(
            chainId = chain.chainId,
            chainType = chain.chainType,
            status = chain.status.name,
            currentStep = chain.currentStep,
            totalSteps = chain.totalSteps,
            failurePolicy = chain.failurePolicy.name,
            lastStepOutput = chain.lastStepOutput,
            errorMessage = chain.errorMessage,
            createdAt = chain.createdAt?.toString(),
            updatedAt = chain.updatedAt?.toString(),
        )
    }
}

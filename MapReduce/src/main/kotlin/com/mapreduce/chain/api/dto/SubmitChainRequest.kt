package com.mapreduce.chain.api.dto

data class SubmitChainRequest(
    val chainType: String,
    val params: String,
)

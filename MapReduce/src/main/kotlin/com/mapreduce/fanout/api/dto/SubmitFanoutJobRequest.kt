package com.mapreduce.fanout.api.dto

data class SubmitFanoutJobRequest(
    val jobType: String,
    val params: String,
)

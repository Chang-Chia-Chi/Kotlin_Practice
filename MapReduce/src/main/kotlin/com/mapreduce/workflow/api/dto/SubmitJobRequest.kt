package com.mapreduce.workflow.api.dto

data class SubmitJobRequest(
    val jobType: String,
    val params: String,
)

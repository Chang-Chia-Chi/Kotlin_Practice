package com.mapreduce.mr.api.dto

data class SubmitJobRequest(
    val jobType: String,
    val params: String,
)

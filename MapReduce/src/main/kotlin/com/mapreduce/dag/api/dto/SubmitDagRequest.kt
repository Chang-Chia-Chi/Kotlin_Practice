package com.mapreduce.dag.api.dto

data class SubmitDagRequest(
    val dagId: String,
    val globalContext: String,
)

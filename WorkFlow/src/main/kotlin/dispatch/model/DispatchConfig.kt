package com.workflow.dispatch.model

data class DispatchConfig(
    val id: String,
    val mode: DispatchMode,
    val algorithmId: String,
    /** Source BOM ID prefix (or full) used by CandidateRepository to filter candidates. */
    val sourceBomPrefix: String,
    val siteTargets: List<SiteTarget>,
    /** Keyed by siteId. LV2 sourceBomId must be full and start with [sourceBomPrefix]. */
    val bomMappings: Map<String, BomMapping>?,
)

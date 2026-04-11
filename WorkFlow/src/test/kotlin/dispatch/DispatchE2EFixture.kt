package com.workflow.dispatch

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.workflow.dispatch.model.Baseline
import com.workflow.dispatch.model.BomMapping
import com.workflow.dispatch.model.CandidateProduct
import com.workflow.dispatch.model.DispatchCategory
import com.workflow.dispatch.model.DispatchConfig
import com.workflow.dispatch.model.DispatchMode
import com.workflow.dispatch.model.SiteBomKey
import com.workflow.dispatch.model.SiteTarget
import com.workflow.dispatch.model.TargetBomAllocation
import java.math.BigDecimal

object DispatchE2EFixture {

    private val mapper = ObjectMapper().registerModule(KotlinModule.Builder().build())
    private val root: JsonNode by lazy {
        val stream = DispatchE2EFixture::class.java.classLoader
            .getResourceAsStream("fixtures/dispatch-e2e-fixture.json")!!
        mapper.readTree(stream)
    }

    fun configs(): List<DispatchConfig> = root["configs"].map { node ->
        DispatchConfig(
            id = node["id"].asText(),
            category = DispatchCategory.valueOf(node["category"].asText()),
            mode = DispatchMode.valueOf(node["mode"].asText()),
            algorithmId = node["algorithmId"].asText(),
            sourceBomPrefix = node["sourceBomPrefix"].asText(),
            siteTargets = node["siteTargets"].map { st ->
                SiteTarget(st["siteId"].asText(), BigDecimal(st["target"].asText()))
            },
            bomMappings = node["bomMappings"]?.takeIf { !it.isNull }?.let { bm ->
                bm.fields().asSequence().associate { (siteId, mapping) ->
                    siteId to BomMapping(
                        sourceBomId = mapping["sourceBomId"].asText(),
                        targetAllocations = mapping["targetAllocations"].map { ta ->
                            TargetBomAllocation(ta["targetBomId"].asText(), BigDecimal(ta["target"].asText()))
                        },
                    )
                }
            },
        )
    }

    fun candidates(configId: String): List<CandidateProduct> =
        root["candidates"][configId].map { node ->
            CandidateProduct(
                productId = node["productId"].asText(),
                sourceBomId = node["sourceBomId"].asText(),
                qty = node["qty"].asInt(),
            )
        }

    fun baseline(configId: String): Baseline {
        val bl = root["baselines"][configId]
        val siteAlloc = bl["siteAllocations"].fields().asSequence()
            .associate { (k, v) -> k to BigDecimal(v.asText()) }
        val bomAlloc = bl["bomAllocations"].fields().asSequence()
            .associate { (k, v) ->
                val (siteId, bomId) = k.split(":")
                SiteBomKey(siteId, bomId) to BigDecimal(v.asText())
            }
        return Baseline(siteAlloc, bomAlloc)
    }

    fun configIds(): List<String> = configs().map { it.id }
}

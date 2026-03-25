package com.workflow.queryexporter.spi

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow

interface LeaderGuard {
    val leaderState: StateFlow<Boolean>

    val isLeader: Boolean get() = leaderState.value

    companion object {
        val ALWAYS: LeaderGuard = object : LeaderGuard {
            override val leaderState: StateFlow<Boolean> = MutableStateFlow(true).asStateFlow()
        }
    }
}

package com.mapreduce.observability

// Superseded by com.mapreduce.observability.health.HealthAggregator
// (LivenessAggregator + ReadinessAggregator) which iterate HealthContributor beans.
//
// The old monolithic LivenessCheck/ReadinessCheck classes have been replaced by
// the health probe registry pattern — see the health/ sub-package.

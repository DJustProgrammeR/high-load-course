package ru.quipy.common.utils.metric

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import org.testcontainers.shaded.com.google.common.util.concurrent.AtomicDouble
import java.util.concurrent.atomic.AtomicInteger

class MetricsCollector(
    private val accountName: String
){

    val actualAvgProcessingTime = AtomicInteger()
    val queueSize = AtomicInteger()
    val cbFailRate = AtomicInteger()
    val cbState = AtomicInteger()

    init {
        Gauge.builder("payment_queued_requests", queueSize) { it.get().toDouble() }
            .tag("account", accountName)
            .register(Metrics.globalRegistry)
        Gauge.builder("actual_avg_processing_time", actualAvgProcessingTime) { it.get().toDouble() }
            .tag("account", accountName)
            .register(Metrics.globalRegistry)
        Gauge.builder("cb_fail_rate", cbFailRate) { it.get().toDouble() }
            .tag("account", accountName)
            .register(Metrics.globalRegistry)
        Gauge.builder("cb_state", cbState) { it.get().toDouble() }
            .tag("account", accountName)
            .register(Metrics.globalRegistry)
    }
}
package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger
import io.micrometer.core.instrument.Metrics


class PaymentExternalServiceMetrics(
    private val accountName: String
){

    val actualAvgProcessingTime = AtomicInteger()
    val queueSize = AtomicInteger()

    init {
        Gauge.builder("payment_queued_requests", queueSize) { it.get().toDouble() }
            .tag("account", accountName)
            .register(Metrics.globalRegistry)
        Gauge.builder("actual_avg_processing_time", actualAvgProcessingTime) { it.get().toDouble() }
            .tag("account", accountName)
            .register(Metrics.globalRegistry)
    }
}
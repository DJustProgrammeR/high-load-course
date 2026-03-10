package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

@Component
class PaymentExternalServiceMetrics(
    registry: MeterRegistry
) {
//    val failures: Counter = Counter.builder("external_service_failures_total")
//        .register(registry)
//
//    val latency: Timer = Timer.builder("external_service_latency")
//        .publishPercentileHistogram()
//        .register(registry)

    val actualAvgProcessingTime = AtomicInteger()
    val queueSize = AtomicInteger()

    init {
        Gauge.builder("payment_queued_requests", queueSize) { it.get().toDouble() }
            .register(registry)
        Gauge.builder("actual_avg_processing_time", actualAvgProcessingTime) { it.get().toDouble() }
            .register(registry)
    }
}
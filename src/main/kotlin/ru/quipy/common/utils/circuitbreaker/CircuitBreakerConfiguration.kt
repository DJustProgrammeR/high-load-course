package ru.quipy.common.utils.circuitbreaker

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.metric.MetricsCollector
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig.SlidingWindowType

@Configuration
public class CircuitBreakerConfiguration {

    @Bean
    fun circuitBreakers(
        properties: List<PaymentAccountProperties>,
        metricsCollectors: Map<String, MetricsCollector?>
    ): Map<String, CircuitBreaker?> =
            properties
                    .associateBy(PaymentAccountProperties::accountName)
            .mapValues {
        when (it.key) {
            "acc-19" -> CircuitBreaker.of(
                "payment-$it.key",
                CircuitBreakerConfig.custom()
                    .slidingWindowType(SlidingWindowType.TIME_BASED)
                    .slidingWindowSize(1)
                    .failureRateThreshold(35.0f)
                    .minimumNumberOfCalls(10)
                    .waitDurationInOpenState(Duration.ofSeconds(3))
                    .permittedNumberOfCallsInHalfOpenState(2)
                    .slowCallDurationThreshold(it.value.averageProcessingTime.multipliedBy(6).dividedBy(5))
                    .build()
            )
                    else -> null
        }
    }
}
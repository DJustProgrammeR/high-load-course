package ru.quipy.common.utils.circuitbreaker

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.metric.MetricsCollector
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration

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
            "acc-19" -> CircuitBreaker(Duration.ofMillis(1000),100, Duration.ofMillis(50),metricsCollectors[it.key] )
                    else -> CircuitBreaker(Duration.ofSeconds(1),100, Duration.ofMillis(100),metricsCollectors[it.key] )
        }
    }
}
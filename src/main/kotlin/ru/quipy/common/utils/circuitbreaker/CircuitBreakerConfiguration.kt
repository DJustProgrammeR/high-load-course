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
                    .slidingWindowSize(2) // За какие N секунд считаем вызовы
                    .failureRateThreshold(30.0f)
                    .minimumNumberOfCalls(10) // Минимальное число вызовов в окне, прежде чем оценивать threshold
                    .waitDurationInOpenState(Duration.ofSeconds(3)) // Сколько остаётся в состоянии OPEN, прежде чем перейти в HALF_OPEN и попробовать пропустить несколько запросов
                    .permittedNumberOfCallsInHalfOpenState(3) // Сколько запросов разрешено пропустить в HALF_OPEN, чтобы проверить ожил ли сервис
                    .build()
            )
                    else -> null
        }
    }
}
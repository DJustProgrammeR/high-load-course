package ru.quipy.common.utils.metric

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.payments.logic.PaymentAccountProperties
import java.time.Duration

@Configuration
public class MetricsConfiguration {

    @Bean
    fun metricsCollectors(
        properties: List<PaymentAccountProperties>
    ): Map<String, MetricsCollector?> =
            properties
                    .associateBy(PaymentAccountProperties::accountName)
            .mapValues {
        when (it.key) {
            "acc-19" -> MetricsCollector(it.key)
                    else -> MetricsCollector(it.key)
        }
    }
}
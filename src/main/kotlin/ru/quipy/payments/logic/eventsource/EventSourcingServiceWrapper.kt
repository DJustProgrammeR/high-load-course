package ru.quipy.payments.logic.eventsource

import kotlinx.coroutines.*
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.PaymentRequest
import ru.quipy.payments.logic.now
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors

@Component
class EventSourcingServiceWrapper(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) {
    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    fun create(paymentId: UUID, orderId: UUID, amount: Int) = dbScope.async {
        val createdEvent = paymentESService.create {
            it.create(
                paymentId,
                orderId,
                amount
            )
        }

        logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
    }

    fun updateLogSubmission(paymentRequest: PaymentRequest) = dbScope.launch {
        paymentESService.update(paymentRequest.paymentId) {
            it.logSubmission(success = true, paymentRequest.transactionId, now(), Duration.ofMillis(now() - paymentRequest.paymentStartedAt))
        }
    }

    fun updateLogProcessing(isSuccess: Boolean, paymentRequest: PaymentRequest, reason: String?) = dbScope.launch {
        paymentESService.update(paymentRequest.paymentId) {
            it.logProcessing(isSuccess, now(), paymentRequest.transactionId, reason = reason)
        }
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(EventSourcingServiceWrapper::class.java)
    }
}

package ru.quipy.payments.logic

import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import ru.quipy.payments.logic.eventsource.EventSourcingServiceWrapper
import java.util.*

@Service
class OrderPayer(
    private val paymentESService: EventSourcingServiceWrapper,
    private val paymentService: PaymentService,
) {

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val (canAccept, delaySeconds) = paymentService.canAcceptPayment(deadline)
        if (!canAccept) {
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                3L.toString()
            )
        }

        val createdAt = now()
        paymentESService.create(paymentId, orderId, amount)

        paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        return createdAt
    }
}
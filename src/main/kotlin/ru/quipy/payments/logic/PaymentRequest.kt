package ru.quipy.payments.logic

import java.util.*

data class PaymentRequest(
    val deadline: Long,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val retryRequestInfo: RetryRequestInfo = RetryRequestInfo(0, now()),
    val transactionId : UUID = UUID.randomUUID()
) : Comparable<PaymentRequest> {
    override fun compareTo(other: PaymentRequest): Int = deadline.compareTo(other.deadline)
}

package ru.quipy.payments.logic

import ru.quipy.payments.logic.PaymentExternalSystemAdapterImpl.RetryRequestData
import java.util.*

data class PaymentRequest(
    val deadline: Long,
    val paymentId: UUID,
    val amount: Int,
    val paymentStartedAt: Long,
    val retryRequestData: RetryRequestData = RetryRequestData(0, now()),
) : Comparable<PaymentRequest> {
    override fun compareTo(other: PaymentRequest): Int = deadline.compareTo(other.deadline)
}

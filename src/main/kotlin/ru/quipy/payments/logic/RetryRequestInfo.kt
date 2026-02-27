package ru.quipy.payments.logic

data class RetryRequestInfo(
    var attempt: Int,
    var startTime: Long
) {
    fun onFailure() {
        attempt = attempt.inc()
    }
}
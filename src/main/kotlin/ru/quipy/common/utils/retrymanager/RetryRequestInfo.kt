package ru.quipy.common.utils.retrymanager

data class RetryRequestInfo(
    var attempt: Int,
    var startTime: Long
) {
    fun onRetryableFailure() {
        ++attempt
    }
}
package ru.quipy.payments.logic


class RetryManager(
    private val maxRetries: Int,
    private val avgProcessingTime: Long = 1000L
) {
    companion object {
        fun multiplier(attempt: Int): Long = listOf<Long>(1, 2, 3, 5, 10)[attempt]

        fun onFailure(attempt: Int): Int {
            return attempt + 1
        }
    }

    fun shouldRetry(currentTime: Long, deadline: Long, attempt: Int): Boolean {
        if (currentTime >= deadline - avgProcessingTime * 1.02) return false
        if (attempt == maxRetries) return false
        return true
    }
}

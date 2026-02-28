package ru.quipy.payments.logic


class RetryManager(
    private val maxRetries: Int,
    private val avgProcessingTime: Long = 1000L
) {
    companion object {
        val multiplierList = listOf<Long>(1, 2, 3, 5, 10)

        fun multiplier(attempt: Int): Long = multiplierList.getOrElse(attempt) { multiplierList.last() }
    }

    fun shouldRetry(currentTime: Long, deadline: Long, attempt: Int): Boolean {
        if (currentTime >= deadline - avgProcessingTime * 1.02) return false
        if (attempt >= maxRetries) return false
        return true
    }
}

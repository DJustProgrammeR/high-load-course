package ru.quipy.payments.logic

import kotlin.math.pow


class RetryManager(
    private val maxRetries: Int,
    private val backoffFactor: Double = 2.0,
    private val jitterMillis: Long = 50L,
    private val avgProcessingTime: Long = 1000L
) {

    fun multiplier(attempt: Int): Long = listOf<Long>(1, 2, 3, 5, 10)[attempt]

    fun shouldRetry(currentTime: Long, deadline: Long, attempt: Int): Boolean {
        if (currentTime >= deadline - avgProcessingTime*1.02) return false
        if (attempt == maxRetries) return false
        return true
    }

    fun computeDelays(startTime: Long, deadline: Long): LongArray? {
        val availableTime = deadline - startTime - avgProcessingTime
        if (availableTime <= 0) {
            return null
        }

        val sumFactor = (backoffFactor.pow(maxRetries - 1) - 1) / (backoffFactor - 1)
        val baseDelay = availableTime / sumFactor
        return LongArray(maxRetries) { i ->
            if (i == maxRetries - 1) 0
            else (baseDelay * backoffFactor.pow(i.toDouble())).toLong()
        }
    }

    suspend fun onFailure(localAttempt: Int): Int {
        val attempt = localAttempt + 1
        return attempt
    }
}

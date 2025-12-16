package ru.quipy.payments.logic

import kotlinx.coroutines.delay
import kotlin.math.pow
import kotlin.random.Random


class RetryManager(
    private val maxRetries: Int,
    private val backoffFactor: Double = 2.0,
    private val jitterMillis: Long = 50L,
    private val avgProcessingTime: Long = 1000L
) {

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

    suspend fun onFailure(localAttempt: Int, delays: LongArray?, startTime: Long): Int {
        val attempt = localAttempt + 1
        val delay = if (attempt < maxRetries) {
            delays?.getOrNull(attempt - 1) ?: 0
        } else {
            0
        }
        val jitter = Random.nextLong(0, jitterMillis + 1)
        val totalDelay = delay + jitter + startTime - System.currentTimeMillis()
        if (totalDelay > 0) {
            delay(totalDelay)
        }
        return attempt
    }
}

package ru.quipy.payments.logic

import kotlin.math.pow
import kotlin.random.Random

class RetryManager(
    private val maxRetries: Int,
    private val backoffFactor: Double = 2.0,
    private val jitterMillis: Long = 50L,
    private val avgProcessingTime: Long = 1000L
) {
    private var deadline: Long = 0
    private var startTime: Long = 0

    fun shouldRetry(deadline: Long, attempt: Int): Boolean {
        if (System.currentTimeMillis() >= deadline) return false
        if (attempt == maxRetries) return false
        return true
    }

    fun computeDelays(deadline: Long, startTime: Long): LongArray {
        val availableTime = deadline - startTime - avgProcessingTime
        if (availableTime <= 0) {
            return LongArray(maxRetries) { 0 }
        }

        val sumFactor = (backoffFactor.pow(maxRetries - 1) - 1) / (backoffFactor - 1)
        val baseDelay = availableTime / sumFactor
        return LongArray(maxRetries) { i ->
            if (i == maxRetries - 1) 0
            else (baseDelay * backoffFactor.pow(i.toDouble())).toLong()
        }
    }

    fun onFailure(attempt: Int, delays: LongArray): Int {
        var curAttempt = attempt + 1
        val delay = if (curAttempt < maxRetries) {
            delays.getOrNull(curAttempt - 1) ?: 0
        } else {
            0
        }
        val jitter = Random.nextLong(0, jitterMillis + 1)
        val totalDelay = delay + jitter + startTime - System.currentTimeMillis()
        if (totalDelay > 0) {
            Thread.sleep(totalDelay)
        }
        return curAttempt
    }
}

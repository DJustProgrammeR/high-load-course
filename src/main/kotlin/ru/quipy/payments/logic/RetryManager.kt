package ru.quipy.payments.logic

import kotlin.math.abs


class RetryManager(
    private val maxRetries: Int,
    private val avgProcessingTime: Long = 1000L,
    private val initialRtt: Double,
    private val maxTimeout: Double
) {
    private val multiplierList = listOf<Long>(1, 2, 3, 5, 10)
    private val alpha = 0.125
    private val beta = 0.25
    @Volatile private var srtt = initialRtt
    @Volatile private var rttvar = initialRtt / 2

    fun getMultiplier(): Long = 1
    fun getScalingMultiplier(attempt: Int): Long = multiplierList.getOrElse(attempt) { multiplierList.last() }

    fun shouldRetry(currentTime: Long, deadline: Long, attempt: Int): Boolean {
        if (currentTime >= deadline - avgProcessingTime * 1.02) return false
        if (attempt >= maxRetries) return false
        return true
    }

    fun recordLatency(latencyMs: Long) {
        val l = latencyMs.toDouble()

        val err = abs(srtt - l)
        rttvar = (1 - beta) * rttvar + beta * err
        srtt = (1 - alpha) * srtt + alpha * l
    }

    fun timeout(): Long {
        val rto = srtt + 4 * rttvar
        return rto.coerceIn(initialRtt / 2, maxTimeout).toLong()
    }
}

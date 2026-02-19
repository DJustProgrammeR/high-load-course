package ru.quipy.payments.logic

import kotlin.math.abs

class AdaptiveTimeout(
    private val initialRtt: Double,
    private val maxTimeout: Double,
) {
    private val alpha = 0.125      // как в TCP
    private val beta = 0.25

    @Volatile private var srtt = initialRtt
    @Volatile private var rttvar = initialRtt / 2

    fun record(latencyMs: Long) {
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
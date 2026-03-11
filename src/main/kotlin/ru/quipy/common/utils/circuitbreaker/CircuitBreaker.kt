package ru.quipy.common.utils.circuitbreaker

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import ru.quipy.common.utils.metric.MetricsCollector
import java.time.Duration
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class CircuitBreaker(
    private val window: Duration,
    private val minFailRate: Int,
    private val waitDuration: Duration,
    private val metrics: MetricsCollector?,
) {
    private val metricsScope = CoroutineScope(Dispatchers.Default)

    @Volatile
    private var openTimestamp = 0L
    @Volatile
    private var closeTimestamp = 0L

    init {
        closeTimestamp = now()
        metricsScope.launch {
            while (isActive) {
                failRate = computeSuccessRate()
                metrics!!.cbFailRate.set(failRate)
                when (state.get()) {
                    State.CLOSED -> metrics.cbState.set(0)
                    State.OPEN -> metrics.cbState.set(2)
                    State.HALF_OPEN -> metrics.cbState.set(1)
                }
                delay(500)
            }
        }
    }

    private enum class State { CLOSED, OPEN, HALF_OPEN }

    private data class Event(
        val timestamp: Long,
//        val started: Boolean,
//        val success: Boolean
    )

    private val events = ArrayDeque<Event>()

    private val state = AtomicReference(State.CLOSED)
    private var failRate = 0

    private val halfOpenTestRunning = AtomicInteger(0)

    private fun now() = System.currentTimeMillis()

    private fun cleanupOldEvents() {
        val limit = now() - window.toMillis()
        while (events.isNotEmpty() && events.first().timestamp < limit) {
            events.removeFirst()
        }
    }

    fun computeSuccessRate(): Int {
        cleanupOldEvents()

        val failed = events.count()
//        val success = events.count { it.success }

//        if (started == 0) return 1.0
        return failed
    }

    fun tryAcquire(): Boolean {

        when (state.get()) {

            State.CLOSED -> return true

            State.OPEN -> {
                val elapsed = now() - openTimestamp

                if (elapsed >= waitDuration.toMillis()) {
                    if (state.compareAndSet(State.OPEN, State.HALF_OPEN)) {
                        return tryAcquireHalfOpen()
                    }
                }

                return false
            }

            State.HALF_OPEN -> {
                return tryAcquireHalfOpen()
            }
        }
    }

    private fun tryAcquireHalfOpen(): Boolean {
        return halfOpenTestRunning.compareAndSet(0, 1)
    }

    fun reportStart() {
        synchronized(events) {
//            events.addLast(Event(now(), started = true, success = false))
        }
    }

    fun reportSuccess() {
        //synchronized(events) {

//            events.addLast(Event(now(), started = false, success = true))
            println("success")
            when (state.get()) {

                State.CLOSED -> {
                    if (failRate >= minFailRate) {
                        state.set(State.OPEN)
                        openTimestamp = now()
                    }
                }

                State.HALF_OPEN -> {

                    state.set(State.CLOSED)

                    halfOpenTestRunning.set(0)
                }

                State.OPEN -> {}
            }
        //}
    }

    fun reportFail() {
        synchronized(events) {

            events.addLast(Event(now()))
            when (state.get()) {

                State.CLOSED -> {}

                State.HALF_OPEN -> {

                    state.set(State.OPEN)
                    openTimestamp = now()

                    halfOpenTestRunning.set(0)
                }

                State.OPEN -> {}
            }
        }
    }
}
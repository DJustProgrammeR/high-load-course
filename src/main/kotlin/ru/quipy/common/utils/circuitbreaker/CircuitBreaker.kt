package ru.quipy.common.utils.circuitbreaker

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import reactor.util.function.Tuple3
import ru.quipy.common.utils.metric.MetricsCollector
import java.time.Duration
import java.util.ArrayDeque
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

class CircuitBreaker(
    private val window: Duration,
    private val minFailRate: Double,
    private val minSlowRate: Double,
    private val waitDuration: Duration,
    private val metrics: MetricsCollector?,
) {
    private val metricsScope = CoroutineScope(Dispatchers.Default)

    @Volatile
    private var openTimestamp = 0L
//    @Volatile
//    private var closeTimestamp = 0L

    private enum class State { CLOSED, OPEN, HALF_OPEN }

    private data class Event(
        val timestamp: Long,
        val fail: Boolean,
        val slow: Boolean,
    )

    private val events = ArrayDeque<Event>()

    private val state = AtomicReference(State.CLOSED)

    private var failRate = 0.0
    private var slowRate = 0.0

    init {
        metrics!!.cbState.set(0)
        metricsScope.launch {
            while (isActive) {
                val (fr, sr) = computeRate()
                failRate = fr
                slowRate = sr
                metrics.cbFailRate.set(failRate)
                metrics.cbSlowRate.set(slowRate)
                when (state.get()) {
                    State.CLOSED -> metrics.cbState.set(0)
                    State.HALF_OPEN -> metrics.cbState.set(1)
                    State.OPEN -> metrics.cbState.set(2)
                }
                delay(200)
            }
        }
    }

    private val halfOpenTestRunning = AtomicInteger(0)

    private fun now() = System.currentTimeMillis()

    private fun cleanupOldEvents() {
        val limit = now() - window.toMillis()
        while (events.isNotEmpty() && events.first().timestamp < limit) {
            events.removeFirst()
        }
    }

    private fun cleanupAllEvents() {
        val limit = now()
        while (events.isNotEmpty() && events.first().timestamp < limit) {
            events.removeFirst()
        }
    }

    fun computeRate(): Pair<Double, Double> {
        cleanupOldEvents()

        val failed = events.count({ it.fail }).toDouble()
        val slow = events.count({ it.slow }).toDouble()

        val general = events.count().toDouble()

        return Pair(failed/general, slow/general)
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

    fun reportSuccess() {
        synchronized(events) {
            events.addLast(Event(now(), fail = false, slow = false))
            when (state.get()) {

                State.CLOSED -> {}

                State.HALF_OPEN -> {

                    state.set(State.CLOSED)
//                    closeTimestamp = now()

                    halfOpenTestRunning.set(0)
                }

                State.OPEN -> {}
            }
        }
    }

    fun reportFail() {
        synchronized(events) {
            events.addLast(Event(now(), fail = true, slow = false))
            when (state.get()) {

                State.CLOSED -> {
                    if ((failRate >= minFailRate || slowRate >= minSlowRate)&& events.count() >= 70L) {
                        state.set(State.OPEN)
                        cleanupAllEvents()
                        openTimestamp = now()
                    }
                }

                State.HALF_OPEN -> {

                    state.set(State.OPEN)
                    cleanupAllEvents()
                    openTimestamp = now()

                    halfOpenTestRunning.set(0)
                }

                State.OPEN -> {}
            }
        }
    }

    fun reportSlow() {
        synchronized(events) {
            events.addLast(Event(now(), fail = false, slow = true))
            when (state.get()) {

                State.CLOSED -> {
                    if ((failRate >= minFailRate || slowRate >= minSlowRate)&& events.count() >= 70L) {
                        state.set(State.OPEN)
                        cleanupAllEvents()
                        openTimestamp = now()
                    }
                }

                State.HALF_OPEN -> {

                    state.set(State.OPEN)
                    cleanupAllEvents()
                    openTimestamp = now()

                    halfOpenTestRunning.set(0)
                }

                State.OPEN -> {}
            }
        }
    }
}
package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

class PaymentDispatchQueue(
    private val rateLimiter: SlidingWindowRateLimiter,
    private val executorScope: CoroutineScope,
    private val parallelRequests: Int,
    private val requestAverageProcessingTime : Duration,
    private val minimalLimitPerSec: Double,
    private val handler: suspend (PaymentRequest) -> Unit
) {
    private val maxQueueSize = 5000
    private val queue = ConcurrentSkipListSet<PaymentRequest>(compareBy { it.deadline })
    private val inFlight = AtomicInteger(0)

    fun enqueue(request: PaymentRequest): Boolean =
        queue.add(request)

    fun size(): Int = queue.size

    fun start(scope: CoroutineScope) {
        scope.launch {
            while (true) {
                poll()
            }
        }
    }

    fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val estimatedWait = queue.size / minimalLimitPerSec
        val willCompleteAt = now() + estimatedWait * 1000 + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = true//  willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, ceil( estimatedWait).toLong())
    }

    private fun poll() {
        if (queue.isEmpty()) return

        if (inFlight.incrementAndGet() > parallelRequests) {
            inFlight.decrementAndGet()
            return
        }

        if (!rateLimiter.tick()) {
            inFlight.decrementAndGet()
            return
        }

        val request = queue.pollFirst() ?: run {
            inFlight.decrementAndGet()
            return
        }

        executorScope.launch {
            try {
                handler(request)
            } finally {
                inFlight.decrementAndGet()
            }
        }
    }
}
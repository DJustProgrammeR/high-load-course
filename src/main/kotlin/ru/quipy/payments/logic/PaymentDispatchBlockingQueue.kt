package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

class PaymentDispatchBlockingQueue(
    private val rateLimiter: SlidingWindowRateLimiter,
    private val executorScope: CoroutineScope,
    private val parallelRequests: Int,
    private val requestAverageProcessingTime : Duration,
    private val minimalLimitPerSec: Double,
    private val handler: suspend (PaymentRequest) -> Unit
) {
    private val maxQueueSize = 5000
    private val queue = PriorityBlockingQueue<PaymentRequest>(maxQueueSize, compareBy { it.deadline })
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

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, ceil( estimatedWait).toLong())
    }


    private fun poll() {
        try {
            val request = queue.take()

            if (!rateLimiter.tick()) {
                queue.offer(request)
                return
            }

            if (inFlight.incrementAndGet() > parallelRequests * 3) { // ???
                inFlight.decrementAndGet()
                queue.offer(request)
                return
            }

            executorScope.launch {
                try {
                    handler(request)
                } finally {
                    inFlight.decrementAndGet()
                }
            }

        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
}
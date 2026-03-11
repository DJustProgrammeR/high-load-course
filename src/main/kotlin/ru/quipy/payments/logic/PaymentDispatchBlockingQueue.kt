package ru.quipy.payments.logic

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import ru.quipy.common.utils.circuitbreaker.CircuitBreaker
import ru.quipy.common.utils.ratelimiter.RateLimiter
import java.util.concurrent.PriorityBlockingQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

class PaymentDispatchBlockingQueue(
    private val rateLimiter: RateLimiter?,
    private val parallelRequests: Int,
    private var requestAverageProcessingTime : Long,
    private val minimalLimitPerSec: Double,
    private val circuitBreaker: CircuitBreaker?,
    private val handler: suspend (PaymentRequest) -> Unit
) {
    @OptIn(DelicateCoroutinesApi::class)
    private val executorScope = CoroutineScope(
        Dispatchers.IO
    )
    private val maxQueueSize = 1000
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

    fun setAverageProcessingTime(time: Long) {
        requestAverageProcessingTime = time
    }

    fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val estimatedWait = queue.size / minimalLimitPerSec // TODO пересчитывать minimalLimitPerSec т.к. requestAverageProcessingTime обновляется
        val willCompleteAt = now() + estimatedWait * 1000 + requestAverageProcessingTime

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, ceil(estimatedWait).toLong())
    }

    private fun poll() {
        try {
            val paymentRequest = queue.poll() ?: return

            if (inFlight.incrementAndGet() > parallelRequests) {
                inFlight.decrementAndGet()
                queue.add(paymentRequest)
                return
            }

            if (!rateLimiter!!.tick()) {
                inFlight.decrementAndGet()
                queue.add(paymentRequest)
                return
            }

            if (!circuitBreaker!!.tryAcquire()) {
                inFlight.decrementAndGet()
                queue.add(paymentRequest)
                return
            }

            executorScope.launch {
                try {
                    handler(paymentRequest)
                } finally {
                    inFlight.decrementAndGet()
                }
            }

        } catch (_: InterruptedException) {
            Thread.currentThread().interrupt()
        }
    }
}
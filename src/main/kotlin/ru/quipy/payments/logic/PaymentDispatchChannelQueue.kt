package ru.quipy.payments.logic

import kotlinx.coroutines.*
import kotlinx.coroutines.channels.Channel
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger
import kotlin.math.ceil

class PaymentDispatchChannelQueue(
    private val rateLimiter: SlidingWindowRateLimiter,
    private val parallelRequests: Int,
    private val requestAverageProcessingTime: Duration,
    private val minimalLimitPerSec: Double,
    private val handler: suspend (PaymentRequest) -> Unit
) {
    private val maxQueueSize = 5000
    private val channel = Channel<PaymentRequest>(capacity = maxQueueSize)
    private val inFlight = AtomicInteger(0)

    fun enqueue(request: PaymentRequest): Boolean {
        return channel.trySend(request).isSuccess
    }

    fun size(): Int = maxQueueSize - inFlight.get()

    fun start(scope: CoroutineScope) {
        repeat(parallelRequests) {
            scope.launch {
                for (request in channel) {
                    while (!rateLimiter.tick()) {
                        delay(1)
                    }

                    try {
                        inFlight.decrementAndGet()
                        handler(request)
                    } finally {
                        inFlight.decrementAndGet()
                    }
                }
            }
        }
    }

    fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val currentSize = size()
        val estimatedWait = currentSize / minimalLimitPerSec
        val willCompleteAt =
            now() + estimatedWait * 1000 + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = currentSize < maxQueueSize

        return Pair(canMeetDeadline && queueOk, ceil(estimatedWait).toLong())
    }
}
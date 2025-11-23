package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.ceil
import kotlin.math.max
import kotlin.math.min


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/(properties.averageProcessingTime.toMillis() / 1000.0)
    private val maxPoolSize = 1100
    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)
    private val responseLatencyHistoryQueueSize = 1100
    private val quantileMap: Map<String, Double> = mapOf(
        "acc-7" to 0.95,
        "acc-12" to 0.99,
        "acc-16" to 0.3
    )

    private val client = OkHttpClient.Builder().build()

    private val responseTime = LinkedBlockingDeque<Long>(responseLatencyHistoryQueueSize)

    private val scheduledExecutorScope = Executors.newSingleThreadExecutor().asCoroutineDispatcher().let { kotlinx.coroutines.CoroutineScope(it) }
    private val targetRps = minimalLimitPerSec
    private val corePoolSize = min(ceil(targetRps / (1000.0 / requestAverageProcessingTime.toMillis())).toInt(),maxPoolSize) //11000

    private val paymentExecutor = ThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(120_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    // Dedicated DB executor for heavy work (parsing, ES updates)
    private val dbExecutor = Executors.newFixedThreadPool(256)

    // Dedicated retry executor as requested (uses ScheduledThreadPool to allow future scheduling if needed)
    private val retryExecutor = ScheduledThreadPoolExecutor(
        max(2, corePoolSize / 8),
        NamedThreadFactory("payment-retry-executor")
    )

    private val retryManager = RetryManager(
        maxRetries = 3,
        backoffFactor = 1.0,
        jitterMillis = 0,
        avgProcessingTime = (requestAverageProcessingTime.toMillis() * 1.05).toLong()
    )

    private data class RequestContext(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long,
        val transactionId: UUID,
        @Volatile var attempt: Int,
        val delays: LongArray,
        @Volatile var lastError: Exception?,
        val requestSubmittedAt: Long = System.currentTimeMillis()
    )

    private val lock = Any()
    private val maxQueueSize = 50000 // hw 9 - 44000 elems approximately
    private val queue = PriorityBlockingQueue<PaymentRequest>(maxQueueSize)

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val inFlightRequests = AtomicInteger(0)

    val queuedRequestsMetric = Gauge.builder("payment_queued_requests", queue) { queue.size.toDouble() }
        .description("Total number of queued requests")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val backPressureGauge = Gauge.builder("payment_backpressure_ratio") {
        (queue.size.toDouble() / maxQueueSize.toDouble()).coerceIn(0.0, 1.0)
    }
        .description("Current back pressure level [0..1]")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val actualTimeout = Counter.builder("payment_actual_timeout_count")
        .description("Count total amount of actual timeouts")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val theoreticalTimeout = Counter.builder("payment_theoretical_timeout_count")
        .description("Count total amount of theoretical timeouts")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val externalPaymentRequests = Counter.builder("external_payment_requests")
        .description("Count total amount of external payment requests")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val externalPaymentRequestsWithRetires = Counter.builder("external_payment_requests_with_retries")
        .description("Count total amount of external payment requests with retires")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    private val q50 = AtomicReference<Double>(0.0)
    private val q80 = AtomicReference<Double>(0.0)
    private val q95 = AtomicReference<Double>(0.0)
    private val q99 = AtomicReference<Double>(0.0)

    init {
        Gauge.builder("external_request_latency", q50) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.5")
            .register(Metrics.globalRegistry)

        Gauge.builder("external_request_latency", q80) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.8")
            .register(Metrics.globalRegistry)

        Gauge.builder("external_request_latency", q95) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.95")
            .register(Metrics.globalRegistry)

        Gauge.builder("external_request_latency", q99) { it.get() }
            .description("External request latency quantiles")
            .tag("accountName", accountName)
            .tag("quantile", "0.99")
            .register(Metrics.globalRegistry)
    }


    override fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val estimatedWaitMs = (queue.size / minimalLimitPerSec) * 1000
        val additionalTimeMs = 1000
        val willCompleteAt = now() + estimatedWaitMs + additionalTimeMs + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, estimatedWaitMs.toLong())
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val paymentRequest = PaymentRequest(deadline) {
            val transactionId = UUID.randomUUID()
            val ctx = RequestContext(
                paymentId = paymentId,
                amount = amount,
                paymentStartedAt = paymentStartedAt,
                deadline = deadline,
                transactionId = transactionId,
                attempt = 0,
                delays = retryManager.computeDelays(deadline, paymentStartedAt),
                lastError = null
            )
            performPaymentWithRetryAsync(ctx)
        }
        var canAccept = Pair(true,0L)
        var accepted = false

        synchronized(lock) {
            canAccept = canAcceptPayment(deadline)
            if (canAccept.first) {
                accepted = queue.offer(paymentRequest)
            }
        }

        if (!canAccept.first) {
            logger.error("429 from PaymentExternalSystemAdapterImpl")
            val delaySeconds = (canAccept.second - System.currentTimeMillis()) / 1000
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                delaySeconds.toString(),
            )
        }

        if (!accepted) {
            logger.error("429 from PaymentExternalSystemAdapterImpl (queue reason)")
            logger.error("[$accountName] Queue overflow! Rejecting payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Queue overflow (back pressure).")
            }
            val retryTimeMs = 5L
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                retryTimeMs.toString()
            )
        }
    }

    // --- Core async path ---
    private fun performPaymentWithRetryAsync(ctx: RequestContext) {
        // This call is the *initial* attempt. It is subject to queue/rps/in-flight limits.
        // We create the HTTP request and send async via OkHttp.

        // Increment in-flight BEFORE sending so the poller accounts for this running request.
        inFlightRequests.incrementAndGet()
        externalPaymentRequests.increment()

        // Record submission in DB asynchronously
        dbExecutor.submit {
            paymentESService.update(ctx.paymentId) {
                it.logSubmission(
                    success = true,
                    ctx.transactionId,
                    now(),
                    Duration.ofMillis(now() - ctx.paymentStartedAt)
                )
            }
        }

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${ctx.transactionId}&paymentId=${ctx.paymentId}&amount=${ctx.amount}")
            post(emptyBody)
        }.build()

        // If deadline already passed, mark as timeout and finish
        if (now() >= ctx.deadline) {
            actualTimeout.increment()
            finalizeFailure(ctx, "Deadline exceeded before submission")
            return
        }

        // If this initial attempt is close to deadline, count as theoretical timeout
        if (now() + requestAverageProcessingTime.toMillis() > ctx.deadline) {
            theoreticalTimeout.increment()
        }

        val clientWithTimeout = buildClientWithTimeout(ctx.deadline)

        clientWithTimeout.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                handleResponseForInitialAttempt(ctx, response)
            }

            override fun onFailure(call: Call, e: IOException) {
                handleFailureForInitialAttempt(ctx, e)
            }
        })
    }

    // Handle response from OkHttp for initial attempt
    private fun handleResponseForInitialAttempt(ctx: RequestContext, response: Response) {
        try {
            val latency = response.receivedResponseAtMillis - response.sentRequestAtMillis
            updateResponseLatencyData(latency)

            val respBodyStr = response.body?.string() ?: ""
            val respCode = response.code
            response.close()

            // heavy parsing and ES update should not run on OkHttp threads
            dbExecutor.submit {
                val body = try {
                    mapper.readValue(respBodyStr, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Bad response for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}, code: ${respCode}, body: $respBodyStr", e)
                    ExternalSysResponse(ctx.transactionId.toString(), ctx.paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}, succeeded: ${body.result}, message: ${body.message}, code: ${respCode}")

                paymentESService.update(ctx.paymentId) {
                    it.logProcessing(body.result, now(), ctx.transactionId, reason = body.message)
                }

                if (body.result) {
                    // success -> finalize and decrement in-flight
                    finalizeSuccess(ctx)
                } else {
                    ctx.lastError = Exception(body.message)
                    when (respCode) {
                        429 -> {
                            scheduleRetryUncontrolled(ctx)
                        }
                        in 400..499 -> {
                            logger.warn("[$accountName] Non-retriable HTTP error $respCode for txId: ${ctx.transactionId}")
                            finalizeFailure(ctx, body.message ?: "HTTP $respCode")
                        }
                        else -> {
                            scheduleRetryUncontrolled(ctx)
                        }
                    }
                }

                updateQuantiles()
            }
        } catch (t: Throwable) {
            // ensure we don't leak inFlight on unexpected exception
            logger.error("[$accountName] Unexpected error handling response", t)
            // treat as failure + retry
            ctx.lastError = Exception(t)
            scheduleRetryUncontrolled(ctx)
        }
    }

    private fun handleFailureForInitialAttempt(ctx: RequestContext, e: IOException) {
        // Don't do heavy work on OkHttp threads
        dbExecutor.submit {
            if (e is SocketTimeoutException) {
                logger.error("[$accountName] Timeout for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}", e)
                ctx.lastError = e
                scheduleRetryUncontrolled(ctx)
            } else {
                logger.error("[$accountName] Payment failed for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}", e)
                ctx.lastError = e
                scheduleRetryUncontrolled(ctx)
            }
        }
    }

    // Schedules a retry on the dedicated retryExecutor. Retries are UNCONTROLLED by RPS/inFlight.
    private fun scheduleRetryUncontrolled(ctx: RequestContext) {
        // Before leaving the in-flight accounted window, decrement so retries are not counted
        inFlightRequests.decrementAndGet()

        // Run retry logic on retryExecutor to avoid blocking OkHttp threads (RetryManager may Thread.sleep)
        retryExecutor.submit {
            try {
                // Use RetryManager.onFailure which blocks/sleeps as it was originally designed
                val nextAttempt = retryManager.onFailure(ctx.attempt, ctx.delays)
                ctx.attempt = nextAttempt

                if (retryManager.shouldRetry(ctx.deadline, ctx.attempt)) {
                    externalPaymentRequestsWithRetires.increment()

                    // Build a new transactionId for retry? Keep the same transactionId to match prior behavior
                    val request = Request.Builder().run {
                        url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${ctx.transactionId}&paymentId=${ctx.paymentId}&amount=${ctx.amount}")
                        post(emptyBody)
                    }.build()

                    val clientWithTimeout = buildClientWithTimeout(ctx.deadline)

                    // Do not increment inFlight — retries are uncontrolled
                    clientWithTimeout.newCall(request).enqueue(object : Callback {
                        override fun onResponse(call: Call, response: Response) {
                            handleResponseForRetry(ctx, response)
                        }

                        override fun onFailure(call: Call, e: IOException) {
                            handleFailureForRetry(ctx, e)
                        }
                    })
                } else {
                    // exhausted retries or deadline passed
                    finalizeFailure(ctx, ctx.lastError?.message ?: "Retries exhausted or deadline")
                }
            } catch (t: Throwable) {
                logger.error("[$accountName] Error running retry logic", t)
                finalizeFailure(ctx, t.message ?: "Retry error")
            }
        }
    }

    private fun handleResponseForRetry(ctx: RequestContext, response: Response) {
        try {
            val latency = response.receivedResponseAtMillis - response.sentRequestAtMillis
            updateResponseLatencyData(latency)

            val respBodyStr = response.body?.string() ?: ""
            val respCode = response.code
            response.close()

            dbExecutor.submit {
                val body = try {
                    mapper.readValue(respBodyStr, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Bad response for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}, code: ${respCode}, body: $respBodyStr", e)
                    ExternalSysResponse(ctx.transactionId.toString(), ctx.paymentId.toString(), false, e.message)
                }

                logger.warn("[$accountName] Retry processed for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}, succeeded: ${body.result}, message: ${body.message}, code: ${respCode}")

                paymentESService.update(ctx.paymentId) {
                    it.logProcessing(body.result, now(), ctx.transactionId, reason = body.message)
                }

                if (body.result) {
                    finalizeSuccess(ctx)
                } else {
                    ctx.lastError = Exception(body.message)
                    when (respCode) {
                        429 -> scheduleRetryUncontrolled(ctx)
                        in 400..499 -> {
                            logger.warn("[$accountName] Non-retriable HTTP error $respCode for txId: ${ctx.transactionId}")
                            finalizeFailure(ctx, body.message ?: "HTTP $respCode")
                        }
                        else -> scheduleRetryUncontrolled(ctx)
                    }
                }

                updateQuantiles()
            }
        } catch (t: Throwable) {
            logger.error("[$accountName] Unexpected error handling retry response", t)
            ctx.lastError = Exception(t)
            scheduleRetryUncontrolled(ctx)
        }
    }

    private fun handleFailureForRetry(ctx: RequestContext, e: IOException) {
        dbExecutor.submit {
            if (e is SocketTimeoutException) {
                logger.error("[$accountName] Timeout on retry for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}", e)
                ctx.lastError = e
                scheduleRetryUncontrolled(ctx)
            } else {
                logger.error("[$accountName] Retry failed for txId: ${ctx.transactionId}, payment: ${ctx.paymentId}", e)
                ctx.lastError = e
                scheduleRetryUncontrolled(ctx)
            }
        }
    }

    private fun finalizeSuccess(ctx: RequestContext) {
        // Successful completion — ensure inFlight is decremented if this was initial attempt and not already decremented
        // Some paths (retry scheduling) already decremented inFlight; guard against double-decrement
        if (inFlightRequests.get() > 0) inFlightRequests.decrementAndGet()
    }

    private fun finalizeFailure(ctx: RequestContext, reason: String) {
        // Log final failure into ES and decrement inFlight if still counted
        paymentESService.update(ctx.paymentId) {
            it.logProcessing(false, now(), ctx.transactionId, reason = reason)
        }
        logger.error("[$accountName] Payment failed after retries for txId: ${ctx.transactionId}, payment: ${ctx.paymentId} — reason: $reason")
        if (inFlightRequests.get() > 0) inFlightRequests.decrementAndGet()
    }

    private fun updateResponseLatencyData(latency: Long) {
        if (responseTime.size >= responseLatencyHistoryQueueSize-1) responseTime.pollFirst()
        responseTime.offerLast(latency)
    }

    private fun buildClientWithTimeout(deadline: Long): OkHttpClient {
        val timeout = calculateQuantiles()[quantileMap[accountName]]?.coerceIn(requestAverageProcessingTime.toMillis(),deadline - now())
        if(timeout != null) {
            val clientWithTimeout = client.newBuilder()
                .callTimeout(Duration.ofMillis(timeout))
                .readTimeout(Duration.ofMillis(timeout))
                .build()
            return clientWithTimeout
        }
        return OkHttpClient()
    }

    private fun calculateQuantiles(): Map<Double, Long> {
        val quantiles = listOf(0.1,0.2,0.3,0.4,0.5, 0.8, 0.95, 0.99)
        val copy = responseTime.toList()
        val result = mutableMapOf<Double, Long>()

        if (copy.isEmpty()) {
            quantiles.forEach { q ->
                result[q] = (q * (requestAverageProcessingTime.toMillis()).toDouble()).toLong()
            }
            return result
        }

        val sorted = copy.sorted()
        quantiles.forEach { q ->
            val index = ((sorted.size - 1) * q).toInt().coerceIn(0, sorted.size - 1)
            result[q] = sorted[index]
        }

        return result
    }

    private fun updateQuantiles() {
        val qs = calculateQuantiles()
        q50.set(qs[0.5]?.toDouble())
        q80.set(qs[0.8]?.toDouble())
        q95.set(qs[0.95]?.toDouble())
        q99.set(qs[0.99]?.toDouble())
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun pollQueue() {
        val paymentRequest = queue.poll() ?: return

        if (inFlightRequests.get() >= parallelRequests) {
            queue.add(paymentRequest)
            return
        }

        if (!outgoingRateLimiter.tick()) {
            queue.add(paymentRequest)
            return
        }

        paymentExecutor.submit {
            try {
                if (now() < paymentRequest.deadline) {
                    if (now() + requestAverageProcessingTime.toMillis() > paymentRequest.deadline) {
                        theoreticalTimeout.increment()
                    }

                    paymentRequest.call.run()

                    if (now() > paymentRequest.deadline) {
                        actualTimeout.increment()
                    }
                } else {
                    actualTimeout.increment()
                }
            } finally {
                // Note: inFlight decrement happens in finalizeSuccess/finalizeFailure when the async HTTP completes or schedules retry
            }
        }
    }


    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            pollQueue()
            Thread.sleep(1)
        }
    }
}

public fun now() = System.currentTimeMillis()
package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
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
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
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
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/properties.averageProcessingTime.toSeconds()
    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)
    private val responseLatencyHistoryQueueSize = 1100
    private val quantileMap: Map<String, Double> = mapOf(
        "acc-7" to 0.95,
        "acc-16" to 0.8
    )

    private val client = OkHttpClient.Builder().build()

    private val responseTime = LinkedBlockingDeque<Long>(responseLatencyHistoryQueueSize)

    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
    private val paymentExecutor = ThreadPoolExecutor(
        16,
        16,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    private val lock = Any()
    private val maxQueueSize = 5000
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
            performPaymentWithRetry(paymentId, amount, paymentStartedAt, deadline)
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

    fun performPaymentWithRetry(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        val retryManager = RetryManager(
            maxRetries = 10,
            backoffFactor = 1.0,
            jitterMillis = 0,
            avgProcessingTime = (requestAverageProcessingTime.toMillis() * 1.05).toLong()
        )

        var lastError: Exception? = null
        var shouldContinue = true

        externalPaymentRequests.increment()
        while (retryManager.shouldRetry(now(), deadline) && shouldContinue) {
            externalPaymentRequestsWithRetires.increment()
            try {
                val clientWithTimeout = buildClientWithTimeout(deadline)

                clientWithTimeout.newCall(request).execute().use { response ->
                    updateResponseLatencyData(response)

                    val respBodyStr = response.body?.string() ?: ""

                    val body = try {
                        mapper.readValue(respBodyStr, ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Bad response for txId: $transactionId, payment: $paymentId, code: ${response.code}, body: $respBodyStr", e)
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, code: ${response.code}")

                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    if (body.result) {
                        shouldContinue = false
                        return
                    } else {
                        lastError = Exception(body.message)
                        when (response.code) {
                            429 -> retryManager.onFailure()
                            in 400..499 -> {
                                logger.warn("[$accountName] Non-retriable HTTP error ${response.code} for txId: $transactionId")
                                shouldContinue = false
                            }
                            else -> retryManager.onFailure()
                        }
                    }
                }
            } catch (e: SocketTimeoutException) {
                logger.error("[$accountName] Timeout for txId: $transactionId, payment: $paymentId", e)
                lastError = e
                retryManager.onFailure()
            } catch (e: Exception) {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                lastError = e
                retryManager.onFailure()
            } finally {
                q50.set(calculateQuantiles()[0.5]?.toDouble())
                q80.set(calculateQuantiles()[0.8]?.toDouble())
                q95.set(calculateQuantiles()[0.95]?.toDouble())
                q99.set(calculateQuantiles()[0.99]?.toDouble())
            }
        }

        if (!shouldContinue) {
            val reason = when {
                now() >= deadline -> "Deadline exceeded."
                lastError != null -> lastError.message ?: "Unknown error"
                else -> "Payment failed after retries."
            }

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = reason)
            }

            logger.error("[$accountName] Payment failed after retries for txId: $transactionId, payment: $paymentId — reason: $reason")
        }
    }

    private fun updateResponseLatencyData(response: Response) {
        val latency = response.receivedResponseAtMillis - response.sentRequestAtMillis
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
        val quantiles = listOf(0.5, 0.8, 0.95, 0.99)
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

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun pollQueue() {
        val paymentRequest = queue.poll() ?: return

        if (inFlightRequests.incrementAndGet() > parallelRequests) {
            inFlightRequests.decrementAndGet()
            queue.add(paymentRequest)
            return
        }

        if (!outgoingRateLimiter.tick()) {
            inFlightRequests.decrementAndGet()
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
                inFlightRequests.decrementAndGet()
            }
        }
    }


    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            pollQueue()
        }
    }
}

public fun now() = System.currentTimeMillis()

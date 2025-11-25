package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.Response
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import io.ktor.client.*
import io.ktor.client.call.body
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.HttpTimeout
import io.ktor.client.plugins.timeout
import io.ktor.client.request.post
import io.ktor.client.statement.HttpResponse
import io.ktor.client.plugins.contentnegotiation.ContentNegotiation
import io.ktor.serialization.jackson.jackson
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
import kotlin.math.ceil
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

        private const val THREAD_COUNT = 1700
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/(properties.averageProcessingTime.toMillis() / 1000.0)
//    private val maxPoolSize = 1100
    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)
    private val responseLatencyHistoryQueueSize = 1100
    private val quantileMap: Map<String, Double> = mapOf(
        "acc-7" to 0.95,
        "acc-12" to 0.99,
        "acc-16" to 0.3
    )

    val client = HttpClient(CIO) {
        engine {
            maxConnectionsCount = 12_000
            endpoint {
                connectTimeout = 2_000          // default connect timeout
                connectAttempts = 5
            }
        }

        install(HttpTimeout) {
            this.requestTimeoutMillis = properties.averageProcessingTime.toMillis()   // allow per-request override
            this.connectTimeoutMillis = properties.averageProcessingTime.toMillis()
            this.socketTimeoutMillis = properties.averageProcessingTime.toMillis()
        }

        install(ContentNegotiation) {
            jackson()
        }
    }

    private val responseTime = LinkedBlockingDeque<Long>(responseLatencyHistoryQueueSize)

    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())
//    private val targetRps = minimalLimitPerSec
//    private val corePoolSize = min(ceil(targetRps / (1000.0 / requestAverageProcessingTime.toMillis())).toInt(),maxPoolSize) //11000
//    private val dbExecutor = ThreadPoolExecutor(
//        256,
//        1024,
//        0L,
//        TimeUnit.MILLISECONDS,
//        LinkedBlockingQueue(120_000),
//        NamedThreadFactory("payment-submission-executor"),
//        CallerBlockingRejectedExecutionHandler()
//    )

    @OptIn(DelicateCoroutinesApi::class)
    private val executorScope = CoroutineScope(
        newFixedThreadPoolContext(THREAD_COUNT, "io_pool")
    )
    val retryManager = RetryManager(
        maxRetries = 2,
        backoffFactor = 1.0,
        jitterMillis = 0,
        avgProcessingTime = (requestAverageProcessingTime.toMillis()).toLong()
    )

    data class RetryRequestData(
        var attempt: Int,
        var delays: LongArray? = null,
        var startTime: Long
    )
//    private val dbExecutor = Executors.newFixedThreadPool(256)

    private val lock = Any()
    private val maxQueueSize = 50000 // hw 9 - 44000 elems approximately
    private val queue = PriorityBlockingQueue<PaymentRequest>(maxQueueSize)

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val inFlightRequests = AtomicInteger(0)

    val queuedRequestsMetric = Gauge.builder("payment_queued_requests", queue) { queue.size.toDouble() }
        .description("Total number of queued requests")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)


    val inFlightMetric = Gauge.builder("in_flight_requests", inFlightRequests) { it.get().toDouble() }
    .description("In flight requests")
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
        val paymentRequest = PaymentRequest(deadline, paymentId, amount, paymentStartedAt)
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

    private suspend fun performPaymentWithRetry(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        try {
            logger.warn("[$accountName] Submitting payment request for payment $paymentId")

            val transactionId = UUID.randomUUID()

//            dbScope.launch {
                paymentESService.update(paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
                }
//            }

            logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()
            val url = request.url.toString()

            var lastError: Exception? = null
            var shouldContinue = true

            var retryRequest = RetryRequestData(0, null, now())
            externalPaymentRequests.increment()
            while (retryManager.shouldRetry(retryRequest.startTime, deadline, retryRequest.attempt) && shouldContinue) {
                retryRequest.delays = retryManager.computeDelays(retryRequest.startTime, deadline)
                externalPaymentRequestsWithRetires.increment()
                try {
                    val timeout = computeDynamicTimeout(deadline)

                    val response: HttpResponse = client.post(url) {
                        timeout {
                            requestTimeoutMillis = timeout
                            socketTimeoutMillis = timeout
                        }
                    }

                    val latency = response.responseTime.timestamp - response.requestTime.timestamp
                    updateResponseLatencyData(latency)

                    val body = runCatching {
                        response.body<ExternalSysResponse>()
                    }.getOrElse {
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentId.toString(),
                            result = false,
                            message = "Bad JSON: ${it.message}"
                        )
                    }

//                val clientWithTimeout = buildClientWithTimeout(deadline)
//
//                clientWithTimeout.newCall(request).execute().use { response ->
//                    updateResponseLatencyData(response)
//
//                    val respBodyStr = response.body?.string() ?: ""
//
//                    val body = try {
//                        mapper.readValue(respBodyStr, ExternalSysResponse::class.java)
//                    } catch (e: Exception) {
//                        logger.error("[$accountName] [ERROR] Bad response for txId: $transactionId, payment: $paymentId, code: ${response.code}, body: $respBodyStr", e)
//                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
//                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, code: ${response.status.value}")

//                    dbScope.launch {
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
//                    }

                    if (body.result) {
                        shouldContinue = false
                        return
                    } else {
                        lastError = Exception(body.message)
                        when (response.status.value) {
                            429 -> {
                                retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
                            }
                            in 400..499 -> {
                                logger.warn("[$accountName] Non-retriable HTTP error ${response.status.value} for txId: $transactionId")
                                shouldContinue = false
                            }
                            else -> {
                                retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
                            }
                        }
                    }
//                }
                } catch (e: SocketTimeoutException) {
                    logger.error("[$accountName] Timeout for txId: $transactionId, payment: $paymentId", e)
                    lastError = e
                    retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
                } catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                    lastError = e
                    retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
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

//                dbScope.launch {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = reason)
                    }
//                }

                logger.error("[$accountName] Payment failed after retries for txId: $transactionId, payment: $paymentId — reason: $reason")
            }

            if (now() > deadline) {
                actualTimeout.increment()
            }
        } finally {
            inFlightRequests.decrementAndGet()
        }
    }

    private fun updateResponseLatencyData(latency: Long) {
        if (responseTime.size >= responseLatencyHistoryQueueSize-1) responseTime.pollFirst()
        responseTime.offerLast(latency)
    }

    private fun computeDynamicTimeout(deadline: Long): Long? {
        val timeout = calculateQuantiles()[quantileMap[accountName]]?.coerceIn((requestAverageProcessingTime.toMillis()*1.5).toLong(),deadline - now())
        return requestAverageProcessingTime.toMillis()
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

        executorScope.launch {
            if (now() < paymentRequest.deadline) {
                if (now() + requestAverageProcessingTime.toMillis() > paymentRequest.deadline) {
                    theoreticalTimeout.increment()
                }

                performPaymentWithRetry(paymentRequest.paymentId, paymentRequest.amount, paymentRequest.paymentStartedAt, paymentRequest.deadline)
            } else {
                actualTimeout.increment()
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

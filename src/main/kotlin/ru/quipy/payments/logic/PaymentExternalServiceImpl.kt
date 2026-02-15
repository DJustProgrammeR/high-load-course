package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.serialization.jackson.*
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.Metrics
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.ConcurrentSkipListSet
import java.util.concurrent.Executors
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
    private val actualRequestAverageProcessingTime = (properties.averageProcessingTime.toMillis().toDouble()*1.0).toLong()
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/(properties.averageProcessingTime.toMillis() / 1000.0)

    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)
//    private val responseLatencyHistoryQueueSize = 1100
    private val quantileMap: Map<String, Double> = mapOf(
        "acc-7" to 0.95,
        "acc-12" to 0.99,
        "acc-16" to 0.3
    )

    val client = HttpClient(Java) {
        engine {
            dispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
            pipelining = true
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }

        install(HttpTimeout) {
            this.requestTimeoutMillis = 2 * properties.averageProcessingTime.toMillis()   // allow per-request override
            this.connectTimeoutMillis = 2 * properties.averageProcessingTime.toMillis()
            this.socketTimeoutMillis = 2 * properties.averageProcessingTime.toMillis()
        }

        install(ContentNegotiation) {
            jackson()
        }
    }

//    private val responseTime = LinkedBlockingDeque<Long>(responseLatencyHistoryQueueSize)

    private val scheduledExecutorScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    @OptIn(DelicateCoroutinesApi::class)
    private val executorScope = CoroutineScope(
        Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    )

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(
        Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
    )

    val retryManager = RetryManager(
        maxRetries = 6,
        backoffFactor = 1.0,
        jitterMillis = 0,
        avgProcessingTime = (actualRequestAverageProcessingTime)
    )

    data class RetryRequestData(
        var attempt: Int,
        var delays: LongArray? = null,
        var startTime: Long
    )


    private val maxQueueSize = 3000 // hw 9 - 44000 elems approximately
    private val queue = ConcurrentSkipListSet<PaymentRequest>(compareBy { it.deadline })

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

    val executorInFlight = AtomicInteger(0)
    val dbInFlight = AtomicInteger(0)
    val inFlightExecutorMetric = Gauge.builder("in_flight_exec_requests", executorInFlight) { it.get().toDouble() }
        .description("In flight exec requests")
        .tag("accountName", accountName)
        .register(Metrics.globalRegistry)

    val inFlightDbMetric = Gauge.builder("in_flight_db_requests", dbInFlight) { it.get().toDouble() }
        .description("In flight db requests")
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
        val additionalTimeMs = 0 // TODO: remove?
        val willCompleteAt = now() + estimatedWaitMs + additionalTimeMs + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, estimatedWaitMs.toLong())
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val paymentRequest = PaymentRequest(deadline, paymentId, amount, paymentStartedAt)
        var canAccept = Pair(true,0L)
        var accepted = false

        canAccept = canAcceptPayment(deadline)
        if (canAccept.first) {
            accepted = queue.add(paymentRequest)
        } else {
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

    private suspend fun performPaymentWithRetry(paymentRequest: PaymentRequest) {
        try {
            logger.warn("[$accountName] Submitting payment request for payment ${paymentRequest.paymentId}")

            val transactionId = UUID.randomUUID()

            dbScope.launch {
                dbInFlight.incrementAndGet()
                paymentESService.update(paymentRequest.paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentRequest.paymentStartedAt))
                }
                dbInFlight.decrementAndGet()
            }

            logger.info("[$accountName] Submit: ${paymentRequest.paymentId} , txId: $transactionId")

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=${paymentRequest.paymentId}&amount=${paymentRequest.amount}")
                post(emptyBody)
            }.build()
            val url = request.url.toString()

            var lastError: Exception? = null
            var shouldContinue = true

            val retryRequest = paymentRequest.retryRequestData
            externalPaymentRequests.increment()
            retryRequest.delays = retryManager.computeDelays(retryRequest.startTime, paymentRequest.deadline)
            while (retryManager.shouldRetry(retryRequest.startTime, paymentRequest.deadline, retryRequest.attempt) && shouldContinue) {
                externalPaymentRequestsWithRetires.increment()
                try {
                    val timeout = computeDynamicTimeout(paymentRequest.deadline)

                    val multiplier = retryManager.multiplier(retryRequest.attempt)
                    val response: HttpResponse = client.post(url) {
                        timeout {
                            requestTimeoutMillis = multiplier * (timeout ?: 10000L)
                            socketTimeoutMillis = multiplier * (timeout ?: 10000L)
                            connectTimeoutMillis = multiplier * (timeout ?: 10000L)
                        }
                    }

//                    val latency = response.responseTime.timestamp - response.requestTime.timestamp
//                    updateResponseLatencyData(latency)

                    val body = runCatching {
                        response.body<ExternalSysResponse>()
                    }.getOrElse {
                        ExternalSysResponse(
                            transactionId.toString(),
                            paymentRequest.paymentId.toString(),
                            result = false,
                            message = "Bad JSON: ${it.message}"
                        )
                    }


                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: ${paymentRequest.paymentId}, succeeded: ${body.result}, message: ${body.message}, code: ${response.status.value}")

                    dbScope.launch {
                        dbInFlight.incrementAndGet()
                        paymentESService.update(paymentRequest.paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                        dbInFlight.decrementAndGet()
                    }

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
                } catch (e: SocketTimeoutException) {
                    logger.error("[$accountName] Timeout for txId: $transactionId, payment: ${paymentRequest.paymentId}", e)
                    lastError = e
                    retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
                }  catch (e: HttpRequestTimeoutException) {
                    logger.error("[$accountName] Timeout for txId: $transactionId, payment: ${paymentRequest.paymentId}", e)
                    lastError = e
                    retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
                }catch (e: Exception) {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: ${paymentRequest.paymentId}", e)
                    lastError = e
                    retryRequest.attempt = retryManager.onFailure(retryRequest.attempt, retryRequest.delays, retryRequest.startTime)
                } finally {
//                    q50.set(calculateQuantiles()[0.5]?.toDouble())
//                    q80.set(calculateQuantiles()[0.8]?.toDouble())
//                    q95.set(calculateQuantiles()[0.95]?.toDouble())
//                    q99.set(calculateQuantiles()[0.99]?.toDouble())
                }
            }

            if (!shouldContinue) {
                val reason = when {
                    now() >= paymentRequest.deadline -> "Deadline exceeded."
                    lastError != null -> lastError.message ?: "Unknown error"
                    else -> "Payment failed after retries."
                }

                dbScope.launch {
                    dbInFlight.incrementAndGet()
                    paymentESService.update(paymentRequest.paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = reason)
                    }
                    dbInFlight.decrementAndGet()
                }

                logger.error("[$accountName] Payment failed after retries for txId: $transactionId, payment: ${paymentRequest.paymentId} — reason: $reason")
            }

            if (now() > paymentRequest.deadline) {
                actualTimeout.increment()
            } else {
                queue.add(paymentRequest)
            }
        } finally {
            inFlightRequests.decrementAndGet()
        }
    }

//    private fun updateResponseLatencyData(latency: Long) {
//        if (responseTime.size >= responseLatencyHistoryQueueSize-1) responseTime.pollFirst()
//        responseTime.offerLast(latency)
//    }

    private fun computeDynamicTimeout(deadline: Long): Long? {
        //val timeout = calculateQuantiles()[quantileMap[accountName]]?.coerceIn((requestAverageProcessingTime.toMillis()*1.5).toLong(),deadline - now())
        return actualRequestAverageProcessingTime.coerceAtMost(deadline - now())
    }

//    private fun calculateQuantiles(): Map<Double, Long> {
//        val quantiles = listOf(0.1,0.2,0.3,0.4,0.5, 0.8, 0.95, 0.99)
//        val copy = responseTime.toList()
//        val result = mutableMapOf<Double, Long>()
//
//        if (copy.isEmpty()) {
//            quantiles.forEach { q ->
//                result[q] = (q * (requestAverageProcessingTime.toMillis()).toDouble()).toLong()
//            }
//            return result
//        }
//
//        val sorted = copy.sorted()
//        quantiles.forEach { q ->
//            val index = ((sorted.size - 1) * q).toInt().coerceIn(0, sorted.size - 1)
//            result[q] = sorted[index]
//        }
//
//        return result
//    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private fun pollQueue() {
        if (queue.isEmpty()) return

        if (inFlightRequests.incrementAndGet() > parallelRequests) {
            inFlightRequests.decrementAndGet()
            return
        }

        if (!outgoingRateLimiter.tick()) {
            inFlightRequests.decrementAndGet()
            return
        }

        val paymentRequest = queue.pollFirst()
        if (paymentRequest == null) {
            inFlightRequests.decrementAndGet()
            return
        }

        executorScope.launch {
            executorInFlight.incrementAndGet()
            if (now() < paymentRequest.deadline) {
                if (now() + requestAverageProcessingTime.toMillis() > paymentRequest.deadline) {
                    theoreticalTimeout.increment()
                }

                performPaymentWithRetry(paymentRequest)
            } else {
                actualTimeout.increment()
            }
            executorInFlight.decrementAndGet()
        }
    }


    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            pollQueue()
        }
    }
}

public fun now() = System.currentTimeMillis()

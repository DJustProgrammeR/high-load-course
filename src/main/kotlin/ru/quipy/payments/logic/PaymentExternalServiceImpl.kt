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
import io.ktor.http.buildUrl
import io.ktor.serialization.jackson.*
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
import kotlin.math.ceil
import kotlin.math.min


// Advice: always treat time as a Duration
@Suppress("Since15")
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
    private val actualRequestAverageProcessingTime = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/(properties.averageProcessingTime.toMillis() / 1000.0)

    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)

    val client = HttpClient(Java) {
        engine {
            dispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
            pipelining = true
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }

        install(HttpTimeout) {
            this.requestTimeoutMillis = 2 * properties.averageProcessingTime.toMillis()
            this.connectTimeoutMillis = 2 * properties.averageProcessingTime.toMillis()
            this.socketTimeoutMillis = 2 * properties.averageProcessingTime.toMillis()
        }

        install(ContentNegotiation) {
            jackson()
        }
    }

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
        maxRetries = 4,
        avgProcessingTime = actualRequestAverageProcessingTime,
        initialRtt = 1.2 * properties.averageProcessingTime.toMillis().toDouble(),
        maxTimeout = 1000.0 // TODO get value from test?
    )

    private val maxQueueSize = 4000
    private val timeoutWhenOverflow = 5L.toString()

    private val queue = ConcurrentSkipListSet<PaymentRequest>(compareBy { it.deadline })

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))
    private val inFlightRequests = AtomicInteger(0)

    override fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        val estimatedWait = queue.size / minimalLimitPerSec
        val willCompleteAt = now() + estimatedWait * 1000 + requestAverageProcessingTime.toMillis()

        val canMeetDeadline = willCompleteAt < deadline
        val queueOk = queue.size < maxQueueSize

        return Pair(canMeetDeadline && queueOk, ceil( estimatedWait).toLong())
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val paymentRequest = PaymentRequest(deadline, paymentId, amount, paymentStartedAt)
        val canAccept = canAcceptPayment(deadline)

        if (!canAccept.first) {
            logger.error("429 from PaymentExternalSystemAdapterImpl")
            val delaySeconds = canAccept.second
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                delaySeconds.toString(),
            )
        }

        if (!queue.add(paymentRequest)) {
            logger.error("[$accountName] Queue overflow! Rejecting payment $paymentId")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Queue overflow (back pressure).")
            }
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                timeoutWhenOverflow
            )
        }
    }

    private suspend fun performPaymentWithRetry(paymentRequest: PaymentRequest) {
        try {
            val transactionId = UUID.randomUUID()
            logger.info("[$accountName] Submitting payment request for payment ${paymentRequest.paymentId}, txId: $transactionId")

            dbScope.launch {
                paymentESService.update(paymentRequest.paymentId) {
                    it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentRequest.paymentStartedAt))
                }
            }

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=${paymentRequest.paymentId}&amount=${paymentRequest.amount}")
                post(emptyBody)
            }.build()
            val url = request.url.toString()

            val retryRequest = paymentRequest.retryRequestInfo
            while (retryManager.shouldRetry(retryRequest, paymentRequest.deadline)) {
                val timeout = computeDynamicTimeout(paymentRequest.deadline)

                when (val result = executeAttempt(paymentRequest, transactionId, url, timeout)) {
                    is AttemptResult.Success -> {
                        return
                    }
                    is AttemptResult.NonRetryableFailure -> {
                        logger.warn("[$accountName] Non-retriable HTTP error ${result.statusCode} for txId: $transactionId")
                        return
                    }
                    is AttemptResult.RetryableFailure -> {
                        retryRequest.onRetryableFailure()
                    }
                }
            }

            val reason = when {
                now() >= paymentRequest.deadline -> "Deadline exceeded after ${retryRequest.attempt} retries"
                else -> "All retry attempts (${retryRequest.attempt}) exhausted"
            }

            logger.error("[$accountName] Payment failed after retries for txId: $transactionId, payment: ${paymentRequest.paymentId} — reason: $reason")

            dbScope.launch {
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(false, now(), transactionId, reason = reason)
                }
            }
        } finally {
            inFlightRequests.decrementAndGet()
        }
    }

    private suspend fun executeAttempt(
        paymentRequest: PaymentRequest,
        transactionId: UUID,
        url: String,
        timeout: Long
    ): AttemptResult {
        return try {
            val multiplier = retryManager.getMultiplier()
            val response: HttpResponse = client.post(url) {
                timeout {
                    requestTimeoutMillis = multiplier * timeout
                    socketTimeoutMillis = multiplier * timeout
                    connectTimeoutMillis = multiplier * timeout
                }
            }

            val latency = response.responseTime.timestamp - response.requestTime.timestamp
            retryManager.recordLatency(latency)

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

            dbScope.launch {
                paymentESService.update(paymentRequest.paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }

            when {
                body.result -> AttemptResult.Success(body)
                response.status.value in 400..499 && response.status.value != 429 ->
                    AttemptResult.NonRetryableFailure(body, response.status.value)
                else -> AttemptResult.RetryableFailure(body)
            }

        } catch (e: SocketTimeoutException) {
            logger.error("Timeout", e)
            retryManager.recordLatency(2 * timeout)
            AttemptResult.RetryableFailure(null, e)
        } catch (e: HttpRequestTimeoutException) {
            logger.error("Timeout", e)
            retryManager.recordLatency(2 * timeout)
            AttemptResult.RetryableFailure(null, e)
        } catch (e: Exception) {
            logger.error("Payment failed", e)
            AttemptResult.RetryableFailure(null, e)
        }
    }

    sealed class AttemptResult {
        data class Success(val body: ExternalSysResponse) : AttemptResult()
        data class NonRetryableFailure(val body: ExternalSysResponse, val statusCode: Int) : AttemptResult()
        data class RetryableFailure(val body: ExternalSysResponse?, val error: Throwable? = null) : AttemptResult()
    }

    private fun computeDynamicTimeout(deadline: Long): Long {
        return retryManager.timeout().coerceAtMost(deadline - now())
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    @Suppress("unused")
    private val releaseJob = scheduledExecutorScope.launch {
        while (true) {
            pollQueue()
        }
    }

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
            if (now() < paymentRequest.deadline) {
                performPaymentWithRetry(paymentRequest)
            }
        }
    }
}

fun now() = System.currentTimeMillis()

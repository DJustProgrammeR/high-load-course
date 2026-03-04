package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.ktor.client.call.*
import io.ktor.client.plugins.*
import io.ktor.client.statement.*
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import ru.quipy.common.utils.ratelimiter.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.ceil
import kotlin.math.min


// Advice: always treat time as a Duration
@Suppress("Since15")
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    paymentProviderHostPort: String,
    token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private var actualAverageProcessingTimeMs = 1000L
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble()/requestAverageProcessingTime.toMillis()
    private val timeoutWhenOverflow = 3L.toString()
    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)

    @OptIn(DelicateCoroutinesApi::class)
    private val executorScope = CoroutineScope(
Dispatchers.IO
    )

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    @OptIn(DelicateCoroutinesApi::class)
    private val metricsScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    private var metricsJob: Job? = null

    private val outgoingRateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1L))

    private val client = PaymentHedgedHttpClient(actualAverageProcessingTimeMs, properties, paymentProviderHostPort, token, 100)

    val retryManager = RetryManager(
        maxTries = 2,
        avgProcessingTimeMs = actualAverageProcessingTimeMs,
        initialRttMs = 1.2 * actualAverageProcessingTimeMs.toDouble(),
        maxTimeoutMs = Duration.ofMillis(1500).toMillis().toDouble()
    )

    private val paymentQueue = PaymentDispatchBlockingQueue(
        outgoingRateLimiter,
        executorScope,
        parallelRequests,
        requestAverageProcessingTime.toMillis() / 2,
        minimalLimitPerSec
    ) { request ->
        performPaymentWithRetry(request)
    }

    init {
        paymentQueue.start(
            CoroutineScope(Executors.newFixedThreadPool(5).asCoroutineDispatcher())
        )

        startMetricsUpdater()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val paymentRequest = PaymentRequest(deadline, paymentId, amount, paymentStartedAt)

        if (!paymentQueue.enqueue(paymentRequest)) {
            logger.error("[$accountName] Queue overflow! Can't equeue $paymentId")
            logProcessing(false, paymentRequest, "Queue overflow (back pressure).")
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                timeoutWhenOverflow
            )
        }
    }

    private suspend fun performPaymentWithRetry(paymentRequest: PaymentRequest) {
        logger.info("[$accountName] Submitting payment request for payment ${paymentRequest.paymentId}, txId: ${paymentRequest.transactionId}")

        dbScope.launch {
            paymentESService.update(paymentRequest.paymentId) {
                it.logSubmission(success = true, paymentRequest.transactionId, now(), Duration.ofMillis(now() - paymentRequest.paymentStartedAt))
            }
        }

        val retryRequest = paymentRequest.retryRequestInfo
        while (retryManager.shouldRetry(retryRequest, paymentRequest.deadline)) {
            val timeout = actualAverageProcessingTimeMs
            val multiplier = retryManager.getMultiplier()

            when (val result = executeAttempt(paymentRequest, ceil(timeout * multiplier).toLong())) {
                is AttemptResult.Success -> {
                    logProcessing(true, paymentRequest, result.body.message)
                    return
                }
                is AttemptResult.NonRetryableFailure -> {
                    logProcessing(false, paymentRequest, result.body.message)
                    logger.warn("[$accountName] Non-retriable HTTP error ${result.statusCode} for txId: ${paymentRequest.transactionId}")
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

        logger.error("[$accountName] Payment failed after retries for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId} — reason: $reason")
        logProcessing(false, paymentRequest, reason)
    }

    private suspend fun executeAttempt(
        paymentRequest: PaymentRequest,
        timeout: Long
    ): AttemptResult {
        return try {
            val response: HttpResponse = client.post(paymentRequest)

            val latency = response.responseTime.timestamp - response.requestTime.timestamp
            retryManager.recordLatency(latency)

            val body = runCatching {
                response.body<ExternalSysResponse>()
            }.getOrElse {
                ExternalSysResponse(
                    paymentRequest.transactionId.toString(),
                    paymentRequest.paymentId.toString(),
                    result = false,
                    message = "Bad JSON: ${it.message}"
                )
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

    private fun logProcessing(isSuccess : Boolean, paymentRequest : PaymentRequest, reason : String?) {
        dbScope.launch {
            paymentESService.update(paymentRequest.paymentId) {
                it.logProcessing(isSuccess, now(), paymentRequest.transactionId, reason = reason)
            }
        }
    }

    sealed class AttemptResult {
        data class Success(val body: ExternalSysResponse) : AttemptResult()
        data class NonRetryableFailure(val body: ExternalSysResponse, val statusCode: Int) : AttemptResult()
        data class RetryableFailure(val body: ExternalSysResponse?, val error: Throwable? = null) : AttemptResult()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        return paymentQueue.canAcceptPayment(deadline)
    }

    private fun startMetricsUpdater() {
        metricsJob = metricsScope.launch {
            while (isActive) {
                try {
                    actualAverageProcessingTimeMs = client.getAverageProcessingTimeMs()
                    paymentQueue.setAverageProcessingTime(actualAverageProcessingTimeMs)
                    retryManager.setAverageProcessingTime(actualAverageProcessingTimeMs)

                } catch (e: Exception) {
                    logger.error("Metrics updater failed", e)
                }

                delay(1000)
            }
        }
    }
}

fun now() = System.currentTimeMillis()

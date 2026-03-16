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
import ru.quipy.common.utils.metric.MetricsCollector
import ru.quipy.common.utils.ratelimiter.RateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.math.min
import io.github.resilience4j.circuitbreaker.CircuitBreaker
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
@Suppress("Since15")
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val metrics: MetricsCollector?,
    private val outgoingRateLimiter: RateLimiter?,
    private val circuitBreaker: CircuitBreaker?
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private var actualAverageProcessingTimeMs = properties.averageProcessingTime.toMillis()
    private val rateLimitPerSec = properties.rateLimitPerSec.toDouble()
    private val parallelRequests = properties.parallelRequests
    private val parallelLimitPerSec = properties.parallelRequests.toDouble() / requestAverageProcessingTime.toMillis()
    private val timeoutWhenOverflow = 3L.toString()
    private val minimalLimitPerSec = min(rateLimitPerSec, parallelLimitPerSec)

    @OptIn(DelicateCoroutinesApi::class)
    private val dbScope = CoroutineScope(
        Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    private val client =
        PaymentHedgedHttpClient(actualAverageProcessingTimeMs, properties, paymentProviderHostPort, token, 100)

    private val retryManager = RetryManager(
        maxTries = 1,
        avgProcessingTimeMs = actualAverageProcessingTimeMs,
        initialRttMs = 1.2 * actualAverageProcessingTimeMs.toDouble(),
        maxTimeoutMs = Duration.ofMillis(1500).toMillis().toDouble()
    )

    private val paymentQueue = PaymentDispatchBlockingQueue(
        outgoingRateLimiter,
        parallelRequests,
        requestAverageProcessingTime.toMillis(),
        minimalLimitPerSec,
        circuitBreaker
    ) { request ->
        performPaymentWithRetry(request)
    }

    private val metricsScope = CoroutineScope(Dispatchers.Default)

    init {
        paymentQueue.start(
            CoroutineScope(Executors.newFixedThreadPool(5).asCoroutineDispatcher())
        )
        metricsScope.launch {
            while (isActive) {
                updateMetrics()
                delay(1000)
            }
        }
    }

    private fun updateMetrics() {
        actualAverageProcessingTimeMs = client.getAverageProcessingTimeMs()
        paymentQueue.setAverageProcessingTime(actualAverageProcessingTimeMs)
        retryManager.setAverageProcessingTime(actualAverageProcessingTimeMs)

        metrics!!.queueSize.set(paymentQueue.size())
        metrics.actualAvgProcessingTime.set(actualAverageProcessingTimeMs.toInt())
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val paymentRequest = PaymentRequest(deadline, paymentId, amount, paymentStartedAt)

        if (!paymentQueue.enqueue(paymentRequest)) {
            //logger.error("[$accountName] Queue overflow! Can't equeue $paymentId")
            logProcessing(false, paymentRequest, "Queue overflow (back pressure).")
            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                timeoutWhenOverflow
            )
        }
    }

    private suspend fun performPaymentWithRetry(paymentRequest: PaymentRequest) {
        //logger.info("[$accountName] Submitting payment request for payment ${paymentRequest.paymentId}, txId: ${paymentRequest.transactionId}")

        logSubmission(paymentRequest)

        val retryRequest = RetryRequestInfo(0, now())
        while (retryManager.shouldRetry(retryRequest, paymentRequest.deadline)) {
            val multiplier = retryManager.getScalingMultiplier(retryRequest.attempt)

            when (val result = executeAttempt(paymentRequest, multiplier/*, timeout * multiplier*/)) {
                is AttemptResult.Success -> {
                    logProcessing(true, paymentRequest, result.body.message)
                    circuitBreaker!!.onSuccess(result.durationMs, TimeUnit.MILLISECONDS)
//                    when {
//                        result.durationMs >= client.getAverageProcessingTimeMs()*1.3 -> circuitBreaker!!.reportSlow()
//                        else -> circuitBreaker!!.reportSuccess()
//                    }
                    return
                }

                is AttemptResult.NonRetryableFailure -> {
                    logProcessing(false, paymentRequest, result.body.message)
                    circuitBreaker!!.onError(
                        now() - retryRequest.startTime,
                        TimeUnit.MILLISECONDS,
                        IllegalStateException("Non-2xx response: ${result.statusCode}")
                    )
                    //logger.warn("[$accountName] Non-retriable HTTP error ${result.statusCode} for txId: ${paymentRequest.transactionId}")
                    return
                }

                is AttemptResult.RetryableFailure -> {
                    circuitBreaker!!.onError(now() - retryRequest.startTime, TimeUnit.MILLISECONDS, result.error?:IllegalStateException("Non-2xx response: ${429}"))
                    retryRequest.onRetryableFailure()
                }
            }
        }

        val reason = when {
            now() >= paymentRequest.deadline -> "Deadline exceeded after ${retryRequest.attempt} retries"
            else -> "All retry attempts (${retryRequest.attempt}) exhausted"
        }


        //paymentQueue.enqueue(paymentRequest)
//        circuitBreaker!!.reportFail()
        logger.error("[$accountName] Payment failed after retries for txId: ${paymentRequest.transactionId}, payment: ${paymentRequest.paymentId} — reason: $reason")
        logProcessing(false, paymentRequest, reason)
    }

    private suspend fun executeAttempt(
        paymentRequest: PaymentRequest,
        multiplier: Long
    ): AttemptResult {
        return try {
            val response: HttpResponse = client.post(paymentRequest, multiplier)

            //val latency = response.responseTime.timestamp - response.requestTime.timestamp
            //retryManager.recordLatency(latency)

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
                body.result -> AttemptResult.Success(
                    body,
                    (response.responseTime.timestamp - response.requestTime.timestamp)
                )

                response.status.value in 400..499 && response.status.value != 429 ->
                    AttemptResult.NonRetryableFailure(body, response.status.value)
                else -> AttemptResult.RetryableFailure(body)
            }
        } catch (e: SocketTimeoutException) {
            //logger.error("Timeout socket", e)
            //retryManager.recordLatency(2 * timeout)
            AttemptResult.RetryableFailure(null, e)
        } catch (e: HttpRequestTimeoutException) {
            //logger.error("Timeout http", e)
            //retryManager.recordLatency(2 * timeout)
            AttemptResult.RetryableFailure(null, e)
        } catch (e: Exception) {
            logger.error("Payment failed", e)
            AttemptResult.RetryableFailure(null, e)
        }
    }

    private fun logSubmission(paymentRequest: PaymentRequest) {
        dbScope.launch {
            paymentESService.update(paymentRequest.paymentId) {
                it.logSubmission(
                    success = true,
                    paymentRequest.transactionId,
                    now(),
                    Duration.ofMillis(now() - paymentRequest.paymentStartedAt)
                )
            }
        }
    }

    private fun logProcessing(isSuccess: Boolean, paymentRequest: PaymentRequest, reason: String?) {
        dbScope.launch {
            paymentESService.update(paymentRequest.paymentId) {
                it.logProcessing(isSuccess, now(), paymentRequest.transactionId, reason = reason)
            }
        }
    }

    sealed class AttemptResult {
        data class Success(val body: ExternalSysResponse, val durationMs: Long) : AttemptResult()
        data class NonRetryableFailure(val body: ExternalSysResponse, val statusCode: Int) : AttemptResult()
        data class RetryableFailure(val body: ExternalSysResponse?, val error: Throwable? = null) : AttemptResult()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    override fun canAcceptPayment(deadline: Long): Pair<Boolean, Long> {
        return paymentQueue.canAcceptPayment(deadline)
    }
}

fun now() = System.currentTimeMillis()

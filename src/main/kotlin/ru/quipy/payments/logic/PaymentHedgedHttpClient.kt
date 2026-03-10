package ru.quipy.payments.logic

import io.ktor.client.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.serialization.jackson.*
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.asCoroutineDispatcher
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.math.ceil
import kotlin.math.min
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.selects.onTimeout

@Suppress("Since15")
class PaymentHedgedHttpClient(
    averageProcessingTimeMs: Long,
    properties: PaymentAccountProperties,
    private val paymentProviderHostPort: String,
    private val token: String,
    windowSize: Int
) {
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val processingTracker = ProcessingTimeTracker(windowSize, averageProcessingTimeMs)

    private val client = HttpClient(Java) {
        engine {
            dispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
            pipelining = true
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }

        install(HttpTimeout) {
            requestTimeoutMillis = 2 * averageProcessingTimeMs
            connectTimeoutMillis = 1L // TODO count general case not local
            // socketTimeoutMillis = 2 * averageProcessingTimeMs TODO настроить сокет и время на установку соединения
        }

        install(ContentNegotiation) {
            jackson()
        }
    }

    fun getAverageProcessingTimeMs(): Long {
        return ceil(processingTracker.average()).toLong()
    }

    @OptIn(ExperimentalCoroutinesApi::class)
    suspend fun post(paymentRequest: PaymentRequest): HttpResponse = coroutineScope {

        val avg = getAverageProcessingTimeMs().coerceAtLeast(1)
        val timeoutMs = 2 * avg
        val hedgeDelayMs = (avg * 0.5).toLong().coerceAtLeast(1)

        val url = "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName" +
                "&token=$token" +
                "&accountName=$accountName" +
                "&transactionId=${paymentRequest.transactionId}" +
                "&paymentId=${paymentRequest.paymentId}" +
                "&amount=${paymentRequest.amount}"

        val primary = async {
            timedRequest(url, timeoutMs)
        }

        val hedged = async(start = CoroutineStart.LAZY) {
            timedRequest(url, timeoutMs)
        }

        val winner = select {
            primary.onAwait { response ->
                response
            }

            onTimeout(hedgeDelayMs) {
                hedged.start()

                select {
                    primary.onAwait { it }
                    hedged.onAwait { it }
                }
            }
        }

        primary.cancel()
        hedged.cancel()

        winner
    }

    private suspend fun timedRequest(
        url: String,
        timeoutMs: Long
    ): HttpResponse {

        val start = System.nanoTime()

        try {
            return client.post(url) {
                timeout {
                    requestTimeoutMillis = timeoutMs
                    //socketTimeoutMillis = timeoutMs TODO настроить сокет и время на установку соединения
                    connectTimeoutMillis = 1L // TODO count general case not local
                }
            }
        } finally {
            val durationMs = (System.nanoTime() - start) / 1_000_000
            processingTracker.add(durationMs)
        }
    }
}

class ProcessingTimeTracker(
    private val windowSize: Int,
    private val startAverageProcessingTime: Long
) {
    private val buffer = AtomicLongArray(windowSize)
    private val counter = AtomicLong(0)
    private val sum = AtomicLong(0)

    init {
        add(startAverageProcessingTime / 2)
    }

    fun add(durationMs: Long) {
        val index = counter.getAndIncrement()
        val slot = (index % windowSize).toInt()

        while (true) {
            val oldValue = buffer.get(slot)
            if (buffer.compareAndSet(slot, oldValue, durationMs)) {
                sum.addAndGet(durationMs - oldValue)
                break
            }
        }
    }

    fun average(): Double {
        val currentCount = min(counter.get(), windowSize.toLong())
        return sum.get().toDouble() / currentCount
    }
}
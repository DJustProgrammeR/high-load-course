package ru.quipy.payments.logic

import io.ktor.client.*
import io.ktor.client.engine.java.*
import io.ktor.client.plugins.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.client.statement.*
import io.ktor.serialization.jackson.*
import kotlinx.coroutines.asCoroutineDispatcher
import java.time.Duration
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongArray
import kotlin.math.ceil
import kotlin.math.min

@Suppress("Since15")
class PaymentHttpClient(
    averageProcessingTime: Duration,
    properties: PaymentAccountProperties,
    private val paymentProviderHostPort: String,
    private val token: String,
    windowSize: Int
) {
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val processingTracker = ProcessingTimeTracker(windowSize, averageProcessingTime.toMillis())

    private val client = HttpClient(Java) {
        engine {
            dispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
            pipelining = true
            protocolVersion = java.net.http.HttpClient.Version.HTTP_2
        }

        install(HttpTimeout) {
            requestTimeoutMillis = 2 * averageProcessingTime.toMillis()
            connectTimeoutMillis = 2 * averageProcessingTime.toMillis()
            socketTimeoutMillis = 2 * averageProcessingTime.toMillis()
        }

        install(ContentNegotiation) {
            jackson()
        }
    }

    fun getAverageProcessingTimeMs(): Long {
        return ceil(processingTracker.average()).toLong()
    }

    suspend fun post(paymentRequest: PaymentRequest, timeoutMs: Long): HttpResponse {
        val url = "http://$paymentProviderHostPort/external/process" +
                "?serviceName=$serviceName" +
                "&token=$token" +
                "&accountName=$accountName" +
                "&transactionId=${paymentRequest.transactionId}" +
                "&paymentId=${paymentRequest.paymentId}" +
                "&amount=${paymentRequest.amount}"

        val start = System.nanoTime()

        try {
            return client.post(url) {
                timeout {
                    requestTimeoutMillis = timeoutMs
                    socketTimeoutMillis = timeoutMs
                    connectTimeoutMillis = timeoutMs
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
        add(startAverageProcessingTime)
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
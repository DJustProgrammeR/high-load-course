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

@Suppress("Since15")
class PaymentHttpClient(
    averageProcessingTime: Duration,
    properties: PaymentAccountProperties,
    private val paymentProviderHostPort: String,
    private val token: String
) {
    private val serviceName = properties.serviceName
    private val accountName = properties.accountName

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

    suspend fun post(paymentRequest : PaymentRequest, timeoutMs: Long): HttpResponse {
        val url = "http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${paymentRequest.transactionId}&paymentId=${paymentRequest.paymentId}&amount=${paymentRequest.amount}"

        return client.post(url) {
            timeout {
                requestTimeoutMillis = timeoutMs
                socketTimeoutMillis = timeoutMs
                connectTimeoutMillis = timeoutMs
            }
        }
    }
}
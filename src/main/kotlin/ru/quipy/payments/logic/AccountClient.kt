package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CustomSlidingWindowRateLimiter
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpClient.Version
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.Semaphore
import java.util.function.Function


class AccountClient(
    private val properties: PaymentAccountProperties,
    private val maxRetries: Int
) {

    companion object {
        val logger = LoggerFactory.getLogger(AccountClient::class.java)

        val emptyBody = HttpRequest.BodyPublishers.ofByteArray(ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val client = HttpClient.newBuilder()
        .version(Version.HTTP_2)
        .executor(Executors.newFixedThreadPool(40))
        .connectTimeout(Duration.ofMillis((properties.averageProcessingTime.toMillis() * 1.4).toLong()))
        .build();

    private val rateLimiter = CustomSlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofMillis(1000)
    )

    private val semaphore = Semaphore(properties.parallelRequests)

    fun sendPayment(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): CompletableFuture<ExternalSysResponse> {
        semaphore.acquire()
        try {
            if (isDeadlineExceeded(deadline)) {
                throw IllegalStateException()
            }
            while (!rateLimiter.tick()) {
                Thread.sleep(20)
            }
            if (isDeadlineExceeded(deadline)) {
                throw IllegalStateException()
            }

            var future = sendInternal(paymentId, amount, transactionId)
            for (i in 1..maxRetries) {
                future =
                    future.thenApply({ value ->
                        CompletableFuture.completedFuture(value)
                    }).exceptionally({ sendInternal(paymentId, amount, transactionId) })
                        .thenCompose(Function.identity())
            }
            return future
        } finally {
            semaphore.release()
        }
    }

    private fun sendInternal(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ): CompletableFuture<ExternalSysResponse> {
        val request = HttpRequest.newBuilder().run {
            uri(
                URI.create(
                    "http://localhost:1234/external/process?" +
                            "serviceName=${properties.serviceName}" +
                            "&accountName=${properties.accountName}" +
                            "&transactionId=$transactionId" +
                            "&paymentId=$paymentId" +
                            "&amount=$amount"
                )
            ).timeout(Duration.ofMillis((properties.averageProcessingTime.toMillis() * 1.4).toLong()))
                .POST(emptyBody)
        }.build()

        return client.sendAsync(request, HttpResponse.BodyHandlers.ofString())
            .thenApply { response ->
                try {
                    mapper.readValue(response.body(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error(
                        "[${properties.accountName}] [ERROR] Payment processed for txId: $transactionId, " +
                                "payment: $paymentId, result code: ${response.statusCode()}, reason: ${response.body()}"
                    )
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }.exceptionally { e ->
                throw e
            }
    }

    private fun isDeadlineExceeded(deadline: Long): Boolean =
        now() + properties.averageProcessingTime.toMillis() * 1.2 >= deadline

}
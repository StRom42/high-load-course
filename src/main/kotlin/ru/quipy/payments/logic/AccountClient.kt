package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CustomSlidingWindowRateLimiter
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

class AccountClient(
    private val properties: PaymentAccountProperties,
    private val maxRetries: Int
) {

    companion object {
        val logger = LoggerFactory.getLogger(AccountClient::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis((properties.averageProcessingTime.toMillis() * 1.4).toLong()))
        .connectionPool(ConnectionPool(properties.parallelRequests, 5, TimeUnit.MINUTES))
        .build()

    private val rateLimiter = CustomSlidingWindowRateLimiter(
        rate = properties.rateLimitPerSec.toLong(),
        window = Duration.ofMillis(1000)
    )

    private val semaphore = Semaphore(properties.parallelRequests)

    suspend fun sendPayment(paymentId: UUID, amount: Int, transactionId: UUID, deadline: Long): ExternalSysResponse {
        return sendInternal(paymentId, amount, transactionId, deadline)
    }

    private suspend fun sendInternal(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        deadline: Long
    ): ExternalSysResponse {
        semaphore.withPermit {
            if (isDeadlineExceeded(deadline)) {
                throw IllegalStateException()
            }
            while (!rateLimiter.tick()) {
                delay(2)
            }
            if (isDeadlineExceeded(deadline)) {
                throw IllegalStateException()
            }
            for (i in 1 .. maxRetries) {
                val result = kotlin.runCatching { sendInternal(paymentId, amount, transactionId) }
                if (result.isSuccess) {
                    return result.getOrThrow()
                }
            }
            throw IllegalStateException()
        }
    }

    private fun sendInternal(paymentId: UUID, amount: Int, transactionId: UUID): ExternalSysResponse {
        val request = Request.Builder().run {
            url(
                "http://localhost:1234/external/process?" +
                        "serviceName=${properties.serviceName}" +
                        "&accountName=${properties.accountName}" +
                        "&transactionId=$transactionId" +
                        "&paymentId=$paymentId" +
                        "&amount=$amount"
            )
            post(emptyBody)
        }.build()

        return client.newCall(request).execute().use { response ->
            try {
                mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error(
                    "[${properties.accountName}] [ERROR] Payment processed for txId: $transactionId, " +
                            "payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}"
                )
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }
        }
    }

    private fun isDeadlineExceeded(deadline: Long): Boolean =
        now() + properties.averageProcessingTime.toMillis() * 1.2 >= deadline

}
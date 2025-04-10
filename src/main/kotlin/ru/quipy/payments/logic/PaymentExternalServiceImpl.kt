package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
    }

    private val accountClient = AccountClient(properties, 1)

    private val accountName = properties.accountName

    private val clientExecutor = Executors.newFixedThreadPool(50)

    private val responseHandlerExecutor = Executors.newFixedThreadPool(40)

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        CompletableFuture.supplyAsync({
            accountClient.sendPayment(paymentId, amount, transactionId, deadline)
        }, clientExecutor).thenComposeAsync {
            it
                .thenApplyAsync(
                    { body ->
                        logger.info(
                            "[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, " +
                                    "succeeded: ${body.result}, message: ${body.message}"
                        )
                        paymentESService.update(paymentId) {
                            it.logProcessing(body.result, now(), transactionId, reason = body.message)
                        }
                    },
                    responseHandlerExecutor
                ).exceptionally(
                    { error ->
                        when (error) {
                            is SocketTimeoutException -> {
                                logger.error(
                                    "[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId",
                                    error
                                )
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                                }
                            }

                            else -> {
                                logger.error(
                                    "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                                    error
                                )
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = error.message)
                                }
                            }
                        }
                    })
        }

    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName
}

public fun now() = System.currentTimeMillis()
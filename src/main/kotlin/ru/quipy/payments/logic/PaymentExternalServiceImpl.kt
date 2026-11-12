package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.MiroRequestQueue
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import kotlin.time.toKotlinDuration


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val queue = MiroRequestQueue(parallelRequests, rateLimitPerSec)
    private val client = OkHttpClient.Builder().build()

    override suspend fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val queueTimeout = Duration.ofMillis(deadline)
            .minusMillis(now())
            .minus(requestAverageProcessingTime)
            .minusSeconds(1)
            .toKotlinDuration()

        val transactionId = UUID.randomUUID()
        val slot = queue.acquire(queueTimeout) {
            recordSubmission(paymentId, paymentStartedAt, transactionId)
        } ?: throw RetryablePaymentException("Payment queue timed out")

        slot.use { submitPayment(paymentId, amount, transactionId) }
    }

    private fun recordSubmission(paymentId: UUID, paymentStartedAt: Long, transactionId: UUID) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")
    }

    private fun submitPayment(paymentId: UUID, amount: Int, transactionId: UUID) {
        fun recordFailure(e: Exception) {
            val reason = if (e is SocketTimeoutException) {
                logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                "Request timeout."
            } else {
                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                e.message
            }

            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = reason)
            }
        }

        val result = try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                val responseBody = response.body?.string()
                try {
                    mapper.readValue(responseBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $responseBody", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
            }
        } catch (e: Exception) {
            recordFailure(e)
            return
        }

        if (!result.result && result.message == "Temporary error") {
            throw RetryablePaymentException("Temporary error. Try again later!")
        }

        logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${result.result}, message: ${result.message}")
        paymentESService.update(paymentId) {
            it.logProcessing(result.result, now(), transactionId, reason = result.message)
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()

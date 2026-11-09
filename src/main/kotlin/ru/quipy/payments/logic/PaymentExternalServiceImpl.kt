package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        private const val COMPLETION_MARGIN_MILLIS = 250L

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofSeconds(30))
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val parallelSemaphore = OngoingWindow(parallelRequests)
    private val pendingReservations = AtomicInteger(0)
    private val inFlight = AtomicInteger(0)
    private val observedProcessingMillis = AtomicLong(0)

    override fun tryReserve(deadline: Long): Boolean {
        while (true) {
            val remaining = deadline - now()
            if (remaining <= 0) return false

            val pending = pendingReservations.get()
            val observed = observedProcessingMillis.get()
            val processingMillis = if (observed > 0) observed else requestAverageProcessingTime.toMillis()

            val waiting = (pending - inFlight.get()).coerceAtLeast(0)
            val rateWait = rateLimiter.estimatedWaitMillis(waiting)
            val parallelWait = (pending - parallelRequests + 1).coerceAtLeast(0) * processingMillis / parallelRequests
            val estimatedDuration = maxOf(rateWait, parallelWait) + processingMillis + COMPLETION_MARGIN_MILLIS

            val coldStartProbe = observed == 0L && pending == 0 && rateWait == 0L &&
                remaining > COMPLETION_MARGIN_MILLIS && processingMillis + COMPLETION_MARGIN_MILLIS >= remaining
            if (estimatedDuration >= remaining && !coldStartProbe) return false
            if (pendingReservations.compareAndSet(pending, pending + 1)) return true
        }
    }

    override fun releaseReservation() {
        pendingReservations.decrementAndGet()
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var permitAcquired = false
        var dispatched = false
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val latestDispatchAt = deadline - observedProcessingMillis.get()
            permitAcquired = parallelSemaphore.tryAcquire(latestDispatchAt - now())
            if (!permitAcquired || !rateLimiter.tickBlocking(latestDispatchAt - now()) || now() >= latestDispatchAt) {
                throw TimeoutException("Insufficient time to complete payment before deadline")
            }

            val startedAt = System.nanoTime()
            inFlight.incrementAndGet()
            dispatched = true
            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] Invalid payment response for txId: $transactionId, payment: $paymentId, result code: ${response.code}", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                if (body.result) {
                    val elapsed = ((System.nanoTime() - startedAt) / 1_000_000).coerceAtLeast(1)
                    observedProcessingMillis.updateAndGet { previous ->
                        if (previous == 0L) elapsed else (previous * 7 + elapsed) / 8
                    }
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            if (e is InterruptedException) Thread.currentThread().interrupt()
        } finally {
            if (dispatched) inFlight.decrementAndGet()
            if (permitAcquired) parallelSemaphore.release()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
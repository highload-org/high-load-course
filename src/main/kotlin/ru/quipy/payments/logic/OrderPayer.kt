package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    @Autowired
    private lateinit var paymentAccounts: List<PaymentExternalSystemAdapter>

    private val paymentExecutor = ThreadPoolExecutor(
        200,
        200,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        ThreadPoolExecutor.AbortPolicy()
    )

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long, receivedAt: Long): Long {
        val paymentAccount = paymentAccounts.single()
        if (!paymentAccount.tryReserve(deadline)) {
            throw RejectedExecutionException("Payment cannot be completed before deadline")
        }

        try {
            paymentExecutor.submit {
                try {
                    val createdEvent = paymentESService.create {
                        it.create(
                            paymentId,
                            orderId,
                            amount
                        )
                    }
                    logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                    paymentService.submitPaymentRequest(paymentId, amount, receivedAt, deadline)
                } finally {
                    paymentAccount.releaseReservation()
                }
            }
        } catch (e: RejectedExecutionException) {
            paymentAccount.releaseReservation()
            throw e
        }
        return receivedAt
    }
}
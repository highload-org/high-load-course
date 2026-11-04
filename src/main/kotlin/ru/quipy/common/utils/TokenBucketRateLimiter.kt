package ru.quipy.common.utils

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

class TokenBucketRateLimiter(
    private val rate: Int,
    private val bucketMaxCapacity: Int,
    private val window: Long,
    private val timeUnit: TimeUnit = TimeUnit.MINUTES,
    startBucket: Int = rate.coerceAtMost(bucketMaxCapacity),
): RateLimiter {
    companion object {
        private val logger: Logger = LoggerFactory.getLogger(TokenBucketRateLimiter::class.java)
    }

    private val rateLimiterScope = CoroutineScope(Executors.newSingleThreadExecutor().asCoroutineDispatcher())

    private var bucket: AtomicInteger = AtomicInteger(startBucket.coerceIn(0, bucketMaxCapacity))

    private val releaseJob = rateLimiterScope.launch {
        val periodMillis = timeUnit.toMillis(window)
        var nextExpectedWakeUp = System.currentTimeMillis() + periodMillis

        while (true) {
            val sleepMillis = nextExpectedWakeUp - System.currentTimeMillis()
            if (sleepMillis > 0) {
                delay(sleepMillis)
            }
            nextExpectedWakeUp += periodMillis

            bucket.get().let { cur ->
                bucket.addAndGet(if (cur + rate > bucketMaxCapacity) bucketMaxCapacity - cur else rate)
            }
        }
    }.invokeOnCompletion { th -> if (th != null) logger.error("Rate limiter release job completed", th) }

    fun availableTokens(): Int = bucket.get()

    override fun tick(): Boolean {
        while (true) {
            val tokensAvailable = bucket.get()
            if (tokensAvailable <= 0) {
                return false
            }
            val res = bucket.compareAndSet(tokensAvailable, tokensAvailable - 1)
            if (res) {
                return true
            }
        }
    }
}
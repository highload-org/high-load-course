package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class LeakingBucketRateLimiter(
    rate: Long,
    window: Duration,
) : RateLimiter {
    private val intervalNanos: Long
    private val lock = ReentrantLock(true)
    private var nextPermitAt = System.nanoTime()

    init {
        require(rate > 0) { "Rate must be positive" }
        require(!window.isZero && !window.isNegative) { "Window must be positive" }
        intervalNanos = (window.toNanos() - 1) / rate + 1
    }

    override fun tick(): Boolean {
        if (!lock.tryLock()) return false
        try {
            val now = System.nanoTime()
            if (nextPermitAt - now > 0) return false
            advanceSchedule(now)
            return true
        } finally {
            lock.unlock()
        }
    }

    @Throws(InterruptedException::class)
    fun tickBlocking() {
        lock.lockInterruptibly()
        try {
            var now = System.nanoTime()
            while (nextPermitAt - now > 0) {
                TimeUnit.NANOSECONDS.sleep(nextPermitAt - now)
                now = System.nanoTime()
            }

            advanceSchedule(now)
        } finally {
            lock.unlock()
        }
    }

    private fun advanceSchedule(now: Long) {
        // Catch up small delays without accumulating permits during longer pauses.
        val scheduledAt = if (now - nextPermitAt >= intervalNanos) now else nextPermitAt
        nextPermitAt = scheduledAt + intervalNanos
    }
}

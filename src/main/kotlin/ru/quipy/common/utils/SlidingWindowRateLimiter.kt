package ru.quipy.common.utils

import java.time.Duration
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock

class SlidingWindowRateLimiter(
    private val rate: Long,
    window: Duration,
) : RateLimiter {
    private val windowNanos = window.toNanos()
    private val permits = ArrayDeque<Long>()
    private val lock = ReentrantLock(true)

    @Volatile
    private var nextPermitAt = System.nanoTime()

    init {
        require(rate > 0 && windowNanos > 0)
    }

    override fun tick(): Boolean {
        if (!lock.tryLock()) return false
        try {
            return acquireOrGetWaitNanos() == 0L
        } finally {
            lock.unlock()
        }
    }

    fun estimatedWaitMillis(queuedAhead: Int): Long {
        val waitForNextPermit = (nextPermitAt - System.nanoTime()).coerceAtLeast(0)
        val waitForQueue = queuedAhead * windowNanos / rate
        return TimeUnit.NANOSECONDS.toMillis(waitForNextPermit + waitForQueue)
    }

    fun tickBlocking() {
        tickBlocking(Long.MAX_VALUE)
    }

    fun tickBlocking(timeoutMillis: Long): Boolean {
        if (timeoutMillis <= 0) return false
        val timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis)
        val startedAt = System.nanoTime()
        if (!lock.tryLock(timeoutNanos, TimeUnit.NANOSECONDS)) return false

        try {
            while (true) {
                val remaining = timeoutNanos - (System.nanoTime() - startedAt)
                if (remaining <= 0) return false

                val waitNanos = acquireOrGetWaitNanos()
                if (waitNanos == 0L) return true
                if (waitNanos >= remaining) return false

                TimeUnit.NANOSECONDS.sleep(waitNanos)
            }
        } finally {
            lock.unlock()
        }
    }

    private fun acquireOrGetWaitNanos(): Long {
        val now = System.nanoTime()
        while (permits.isNotEmpty() && now - permits.first() >= windowNanos) {
            permits.removeFirst()
        }
        if (permits.size.toLong() >= rate) return windowNanos - (now - permits.first())

        permits.addLast(now)
        nextPermitAt = if (permits.size.toLong() == rate) permits.first() + windowNanos else now
        return 0
    }
}

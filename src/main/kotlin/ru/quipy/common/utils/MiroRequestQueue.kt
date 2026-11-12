package ru.quipy.common.utils

import kotlinx.coroutines.delay
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.TimeSource

class MiroRequestQueue(parallelRequests: Int, rateLimitPerSec: Int) {
    init {
        require(rateLimitPerSec > 0)
    }

    private val pending = Mutex()
    private val slots = Semaphore(parallelRequests)
    private val interval = ((1_000_000_000L + rateLimitPerSec - 1) / rateLimitPerSec).nanoseconds
    private var lastSubmission: TimeSource.Monotonic.ValueTimeMark? = null

    suspend fun acquire(timeout: Duration, beforeSend: () -> Unit): AutoCloseable? {
        var locked = false
        var slot: AutoCloseable? = null
        try {
            val acquired = withTimeoutOrNull(timeout) {
                pending.lock()
                locked = true
                waitForInterval()
                acquireSlot().also { slot = it }
            } ?: return null

            beforeSend()
            lastSubmission = TimeSource.Monotonic.markNow()
            slot = null
            return acquired
        } finally {
            // A timeout can win after a resource is acquired but before the block returns.
            slot?.close()
            if (locked) pending.unlock()
        }
    }

    private suspend fun waitForInterval() {
        val previous = lastSubmission ?: return
        while (true) {
            val remaining = interval - previous.elapsedNow()
            if (remaining <= Duration.ZERO) return
            delay(remaining)
        }
    }

    private suspend fun acquireSlot(): AutoCloseable {
        slots.acquire()
        val released = AtomicBoolean()
        return AutoCloseable {
            if (released.compareAndSet(false, true)) slots.release()
        }
    }
}

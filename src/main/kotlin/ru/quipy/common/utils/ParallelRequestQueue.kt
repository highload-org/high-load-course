package ru.quipy.common.utils

import kotlinx.coroutines.sync.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

class ParallelRequestQueue(parallelRequests: Int) {
    private val slots = Semaphore(parallelRequests)

    suspend fun acquire(): AutoCloseable {
        slots.acquire()
        val released = AtomicBoolean()
        return AutoCloseable {
            if (released.compareAndSet(false, true)) slots.release()
        }
    }
}

package ru.quipy.common.utils

import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class OrderServiceExceptionHandler {
    @ExceptionHandler(TooManyRequestsException::class)
    fun handleTooManyRequests(exception: TooManyRequestsException): ResponseEntity<Void> {
        return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
            .header(HttpHeaders.RETRY_AFTER, exception.retryAfter.toString())
            .build()
    }
}

class TooManyRequestsException(val retryAfter: Long) : RuntimeException()

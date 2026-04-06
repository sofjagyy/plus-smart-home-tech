package ru.yandex.practicum.delivery.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.interaction.api.exception.ApiException;

import java.util.Map;

@RestControllerAdvice
public class ErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(ErrorHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, String>> handleApiException(ApiException e) {
        log.warn("{} {}", e.getHttpStatus().value(), e.getMessage());
        return ResponseEntity.status(e.getHttpStatus())
                .body(Map.of(
                        "httpStatus", e.getHttpStatus().toString(),
                        "userMessage", e.getUserMessage(),
                        "message", e.getMessage()
                ));
    }
}

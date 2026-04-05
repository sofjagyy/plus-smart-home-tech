package ru.yandex.practicum.delivery.controller;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import ru.yandex.practicum.interaction.api.exception.ApiException;

import java.util.Map;

@RestControllerAdvice
public class ErrorHandler {

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, String>> handleApiException(ApiException e) {
        return ResponseEntity.status(e.getHttpStatus())
                .body(Map.of(
                        "httpStatus", e.getHttpStatus().toString(),
                        "userMessage", e.getUserMessage(),
                        "message", e.getMessage()
                ));
    }
}

package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class ProductNotFoundException extends ApiException {
    public ProductNotFoundException(String message) {
        super(message, HttpStatus.NOT_FOUND, message);
    }
}

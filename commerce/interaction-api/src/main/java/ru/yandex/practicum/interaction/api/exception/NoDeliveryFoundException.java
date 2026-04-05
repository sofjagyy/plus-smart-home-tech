package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class NoDeliveryFoundException extends ApiException {
    public NoDeliveryFoundException(String message) {
        super(message, HttpStatus.NOT_FOUND, message);
    }
}

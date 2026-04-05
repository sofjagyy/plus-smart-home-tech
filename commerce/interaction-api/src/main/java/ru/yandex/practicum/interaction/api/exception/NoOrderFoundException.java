package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class NoOrderFoundException extends ApiException {
    public NoOrderFoundException(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

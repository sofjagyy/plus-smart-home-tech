package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class NotAuthorizedUserException extends ApiException {
    public NotAuthorizedUserException(String message) {
        super(message, HttpStatus.UNAUTHORIZED, message);
    }
}

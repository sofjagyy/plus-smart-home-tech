package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class NotEnoughInfoInOrderToCalculateException extends ApiException {
    public NotEnoughInfoInOrderToCalculateException(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

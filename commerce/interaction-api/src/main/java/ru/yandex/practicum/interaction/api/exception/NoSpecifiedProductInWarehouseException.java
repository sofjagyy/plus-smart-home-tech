package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class NoSpecifiedProductInWarehouseException extends ApiException {
    public NoSpecifiedProductInWarehouseException(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

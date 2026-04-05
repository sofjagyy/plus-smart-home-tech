package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class SpecifiedProductAlreadyInWarehouseException extends ApiException {
    public SpecifiedProductAlreadyInWarehouseException(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

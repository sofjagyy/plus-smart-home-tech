package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class NoProductsInShoppingCartException extends ApiException {
    public NoProductsInShoppingCartException(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

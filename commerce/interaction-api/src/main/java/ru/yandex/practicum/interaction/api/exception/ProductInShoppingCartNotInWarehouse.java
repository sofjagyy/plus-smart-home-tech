package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class ProductInShoppingCartNotInWarehouse extends ApiException {
    public ProductInShoppingCartNotInWarehouse(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

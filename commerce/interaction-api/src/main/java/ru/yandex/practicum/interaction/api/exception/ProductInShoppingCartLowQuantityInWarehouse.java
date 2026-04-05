package ru.yandex.practicum.interaction.api.exception;

import org.springframework.http.HttpStatus;

public class ProductInShoppingCartLowQuantityInWarehouse extends ApiException {
    public ProductInShoppingCartLowQuantityInWarehouse(String message) {
        super(message, HttpStatus.BAD_REQUEST, message);
    }
}

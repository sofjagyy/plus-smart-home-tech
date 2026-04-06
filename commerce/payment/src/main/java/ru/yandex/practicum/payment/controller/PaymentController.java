package ru.yandex.practicum.payment.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.interaction.api.client.PaymentClient;
import ru.yandex.practicum.interaction.api.dto.OrderDto;
import ru.yandex.practicum.interaction.api.dto.PaymentDto;
import ru.yandex.practicum.payment.service.PaymentService;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
public class PaymentController implements PaymentClient {

    private final PaymentService service;

    @Override
    @PostMapping
    public PaymentDto payment(@RequestBody OrderDto order) {
        return service.payment(order);
    }

    @Override
    @PostMapping("/productCost")
    public BigDecimal productCost(@RequestBody OrderDto order) {
        return service.productCost(order);
    }

    @Override
    @PostMapping("/totalCost")
    public BigDecimal getTotalCost(@RequestBody OrderDto order) {
        return service.getTotalCost(order);
    }

    @Override
    @PostMapping("/refund")
    public void paymentSuccess(@RequestBody UUID paymentId) {
        service.paymentSuccess(paymentId);
    }

    @Override
    @PostMapping("/failed")
    public void paymentFailed(@RequestBody UUID paymentId) {
        service.paymentFailed(paymentId);
    }
}

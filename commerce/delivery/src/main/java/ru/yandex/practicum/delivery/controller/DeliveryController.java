package ru.yandex.practicum.delivery.controller;

import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.delivery.service.DeliveryService;
import ru.yandex.practicum.interaction.api.client.DeliveryClient;
import ru.yandex.practicum.interaction.api.dto.DeliveryDto;
import ru.yandex.practicum.interaction.api.dto.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
public class DeliveryController implements DeliveryClient {

    private final DeliveryService service;

    @Override
    @PutMapping
    public DeliveryDto planDelivery(@RequestBody DeliveryDto delivery) {
        return service.planDelivery(delivery);
    }

    @Override
    @PostMapping("/cost")
    public BigDecimal deliveryCost(@RequestBody OrderDto order) {
        return service.deliveryCost(order);
    }

    @Override
    @PostMapping("/picked")
    public void deliveryPicked(@RequestBody UUID orderId) {
        service.deliveryPicked(orderId);
    }

    @Override
    @PostMapping("/successful")
    public void deliverySuccessful(@RequestBody UUID orderId) {
        service.deliverySuccessful(orderId);
    }

    @Override
    @PostMapping("/failed")
    public void deliveryFailed(@RequestBody UUID orderId) {
        service.deliveryFailed(orderId);
    }
}

package ru.yandex.practicum.delivery.service;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.delivery.entity.Delivery;
import ru.yandex.practicum.delivery.repository.DeliveryRepository;
import ru.yandex.practicum.interaction.api.client.OrderClient;
import ru.yandex.practicum.interaction.api.client.WarehouseClient;
import ru.yandex.practicum.interaction.api.dto.AddressDto;
import ru.yandex.practicum.interaction.api.dto.DeliveryDto;
import ru.yandex.practicum.interaction.api.dto.OrderDto;
import ru.yandex.practicum.interaction.api.dto.ShippedToDeliveryRequest;
import ru.yandex.practicum.interaction.api.enums.DeliveryState;
import ru.yandex.practicum.interaction.api.exception.NoDeliveryFoundException;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class DeliveryService {

    private static final Logger log = LoggerFactory.getLogger(DeliveryService.class);
    private static final double BASE_COST = 5.0;

    private final DeliveryRepository deliveryRepository;
    private final OrderClient orderClient;
    private final WarehouseClient warehouseClient;

    @Transactional
    public DeliveryDto planDelivery(DeliveryDto dto) {
        Delivery delivery = Delivery.builder()
                .orderId(dto.getOrderId())
                .deliveryState(DeliveryState.CREATED)
                .build();

        if (dto.getFromAddress() != null) {
            delivery.setFromCountry(dto.getFromAddress().getCountry());
            delivery.setFromCity(dto.getFromAddress().getCity());
            delivery.setFromStreet(dto.getFromAddress().getStreet());
            delivery.setFromHouse(dto.getFromAddress().getHouse());
            delivery.setFromFlat(dto.getFromAddress().getFlat());
        }

        if (dto.getToAddress() != null) {
            delivery.setToCountry(dto.getToAddress().getCountry());
            delivery.setToCity(dto.getToAddress().getCity());
            delivery.setToStreet(dto.getToAddress().getStreet());
            delivery.setToHouse(dto.getToAddress().getHouse());
            delivery.setToFlat(dto.getToAddress().getFlat());
        }

        return toDto(deliveryRepository.save(delivery));
    }

    public BigDecimal deliveryCost(OrderDto order) {
        UUID orderId = order.getOrderId();
        Delivery delivery = findByOrderId(orderId);
        String warehouseStreet = delivery.getFromStreet();
        String toStreet = delivery.getToStreet();

        log.info(
                "deliveryCost orderId={} in fragile={} weight={} volume={} fromStreet={} toStreet={} deliveryId={}",
                orderId,
                order.isFragile(),
                order.getDeliveryWeight(),
                order.getDeliveryVolume(),
                warehouseStreet,
                toStreet,
                delivery.getDeliveryId());

        double cost = BASE_COST;

        int multiplier = warehouseStreet != null && warehouseStreet.contains("ADDRESS_1") ? 1 : 2;
        cost = cost * multiplier + BASE_COST;

        if (order.isFragile()) {
            cost += cost * 0.2;
        }

        cost += order.getDeliveryWeight() * 0.3;
        cost += order.getDeliveryVolume() * 0.2;

        if (toStreet == null || !toStreet.equals(warehouseStreet)) {
            cost += cost * 0.2;
        }

        BigDecimal result = BigDecimal.valueOf(cost);
        log.info("deliveryCost orderId={} out cost={} multiplier={}", orderId, result, multiplier);
        return result;
    }

    @Transactional
    public void deliveryPicked(UUID orderId) {
        Delivery delivery = findByOrderId(orderId);
        delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
        deliveryRepository.save(delivery);

        orderClient.assembly(orderId);
        warehouseClient.shippedToDelivery(new ShippedToDeliveryRequest(orderId, delivery.getDeliveryId()));
    }

    @Transactional
    public void deliverySuccessful(UUID orderId) {
        Delivery delivery = findByOrderId(orderId);
        delivery.setDeliveryState(DeliveryState.DELIVERED);
        deliveryRepository.save(delivery);
        orderClient.delivery(orderId);
    }

    @Transactional
    public void deliveryFailed(UUID orderId) {
        Delivery delivery = findByOrderId(orderId);
        delivery.setDeliveryState(DeliveryState.FAILED);
        deliveryRepository.save(delivery);
        orderClient.deliveryFailed(orderId);
    }

    private Delivery findByOrderId(UUID orderId) {
        return deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundException("Delivery not found for order: " + orderId));
    }

    private DeliveryDto toDto(Delivery delivery) {
        return DeliveryDto.builder()
                .deliveryId(delivery.getDeliveryId())
                .fromAddress(AddressDto.builder()
                        .country(delivery.getFromCountry())
                        .city(delivery.getFromCity())
                        .street(delivery.getFromStreet())
                        .house(delivery.getFromHouse())
                        .flat(delivery.getFromFlat())
                        .build())
                .toAddress(AddressDto.builder()
                        .country(delivery.getToCountry())
                        .city(delivery.getToCity())
                        .street(delivery.getToStreet())
                        .house(delivery.getToHouse())
                        .flat(delivery.getToFlat())
                        .build())
                .orderId(delivery.getOrderId())
                .deliveryState(delivery.getDeliveryState())
                .build();
    }
}

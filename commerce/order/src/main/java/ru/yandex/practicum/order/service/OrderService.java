package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.interaction.api.client.DeliveryClient;
import ru.yandex.practicum.interaction.api.client.PaymentClient;
import ru.yandex.practicum.interaction.api.client.WarehouseClient;
import ru.yandex.practicum.interaction.api.dto.*;
import ru.yandex.practicum.interaction.api.enums.DeliveryState;
import ru.yandex.practicum.interaction.api.enums.OrderState;
import ru.yandex.practicum.interaction.api.exception.NoOrderFoundException;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class OrderService {

    private final OrderRepository orderRepository;
    private final WarehouseClient warehouseClient;
    private final PaymentClient paymentClient;
    private final DeliveryClient deliveryClient;

    @Transactional
    public OrderDto createNewOrder(CreateNewOrderRequest request) {
        Order order = Order.builder()
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(request.getShoppingCart().getProducts())
                .state(OrderState.NEW)
                .build();

        if (request.getDeliveryAddress() != null) {
            order.setDeliveryCountry(request.getDeliveryAddress().getCountry());
            order.setDeliveryCity(request.getDeliveryAddress().getCity());
            order.setDeliveryStreet(request.getDeliveryAddress().getStreet());
            order.setDeliveryHouse(request.getDeliveryAddress().getHouse());
            order.setDeliveryFlat(request.getDeliveryAddress().getFlat());
        }

        return toDto(orderRepository.save(order));
    }

    @Transactional(readOnly = true)
    public List<OrderDto> getClientOrders(String username) {
        return orderRepository.findAll().stream()
                .map(this::toDto)
                .collect(Collectors.toList());
    }

    @Transactional
    public OrderDto assembly(UUID orderId) {
        Order order = findOrder(orderId);

        AssemblyProductsForOrderRequest assemblyRequest = new AssemblyProductsForOrderRequest(
                order.getProducts(), order.getOrderId());
        BookedProductsDto booked = warehouseClient.assemblyProductsForOrder(assemblyRequest);

        order.setDeliveryWeight(booked.getDeliveryWeight());
        order.setDeliveryVolume(booked.getDeliveryVolume());
        order.setFragile(booked.isFragile());

        AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
        AddressDto deliveryAddress = AddressDto.builder()
                .country(order.getDeliveryCountry())
                .city(order.getDeliveryCity())
                .street(order.getDeliveryStreet())
                .house(order.getDeliveryHouse())
                .flat(order.getDeliveryFlat())
                .build();

        DeliveryDto deliveryDto = DeliveryDto.builder()
                .fromAddress(warehouseAddress)
                .toAddress(deliveryAddress)
                .orderId(order.getOrderId())
                .deliveryState(DeliveryState.CREATED)
                .build();

        DeliveryDto createdDelivery = deliveryClient.planDelivery(deliveryDto);
        order.setDeliveryId(createdDelivery.getDeliveryId());
        order.setState(OrderState.ASSEMBLED);

        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        Order order = findOrder(orderId);
        OrderDto orderDto = toDto(order);
        order.setDeliveryPrice(deliveryClient.deliveryCost(orderDto));
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        Order order = findOrder(orderId);
        OrderDto orderDto = toDto(order);
        order.setTotalPrice(paymentClient.getTotalCost(orderDto));
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto payment(UUID orderId) {
        Order order = findOrder(orderId);
        OrderDto orderDto = toDto(order);
        order.setProductPrice(paymentClient.productCost(orderDto));

        orderDto = toDto(order);
        PaymentDto paymentDto = paymentClient.payment(orderDto);
        order.setPaymentId(paymentDto.getPaymentId());
        order.setState(OrderState.ON_PAYMENT);

        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.PAYMENT_FAILED);
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto delivery(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.DELIVERED);
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.DELIVERY_FAILED);
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.ASSEMBLY_FAILED);
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto complete(UUID orderId) {
        Order order = findOrder(orderId);
        order.setState(OrderState.COMPLETED);
        return toDto(orderRepository.save(order));
    }

    @Transactional
    public OrderDto productReturn(ProductReturnRequest request) {
        Order order = findOrder(request.getOrderId());
        warehouseClient.acceptReturn(request.getProducts());
        order.setState(OrderState.PRODUCT_RETURNED);
        return toDto(orderRepository.save(order));
    }

    private Order findOrder(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundException("Order not found: " + orderId));
    }

    private OrderDto toDto(Order order) {
        return OrderDto.builder()
                .orderId(order.getOrderId())
                .shoppingCartId(order.getShoppingCartId())
                .products(order.getProducts())
                .paymentId(order.getPaymentId())
                .deliveryId(order.getDeliveryId())
                .state(order.getState())
                .deliveryWeight(order.getDeliveryWeight())
                .deliveryVolume(order.getDeliveryVolume())
                .fragile(order.isFragile())
                .totalPrice(order.getTotalPrice())
                .deliveryPrice(order.getDeliveryPrice())
                .productPrice(order.getProductPrice())
                .build();
    }
}

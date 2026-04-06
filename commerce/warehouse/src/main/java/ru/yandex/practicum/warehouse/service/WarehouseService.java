package ru.yandex.practicum.warehouse.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.interaction.api.client.ShoppingStoreClient;
import ru.yandex.practicum.interaction.api.dto.*;
import ru.yandex.practicum.interaction.api.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.interaction.api.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.interaction.api.exception.SpecifiedProductAlreadyInWarehouseException;
import ru.yandex.practicum.warehouse.entity.OrderBooking;
import ru.yandex.practicum.warehouse.entity.WarehouseProduct;
import ru.yandex.practicum.warehouse.repository.OrderBookingRepository;
import ru.yandex.practicum.warehouse.repository.WarehouseProductRepository;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class WarehouseService {

    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS =
            ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    private final WarehouseProductRepository repository;
    private final OrderBookingRepository orderBookingRepository;
    private final ShoppingStoreClient shoppingStoreClient;

    @Transactional
    public void newProduct(NewProductInWarehouseRequest request) {
        if (repository.existsById(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseException(
                    "Product already in warehouse: " + request.getProductId());
        }

        try {
            shoppingStoreClient.getProduct(request.getProductId());
        } catch (FeignException e) {
            if (e.status() == 404) {
                throw new NoSpecifiedProductInWarehouseException(
                        "Product not found in store: " + request.getProductId());
            }
            throw e;
        }

        WarehouseProduct product = WarehouseProduct.builder()
                .productId(request.getProductId())
                .fragile(request.isFragile())
                .width(request.getDimension().getWidth())
                .height(request.getDimension().getHeight())
                .depth(request.getDimension().getDepth())
                .weight(request.getWeight())
                .quantity(0)
                .build();
        repository.save(product);
    }

    @Transactional
    public void addProduct(AddProductToWarehouseRequest request) {
        WarehouseProduct product = findProduct(request.getProductId());
        product.setQuantity(product.getQuantity() + request.getQuantity());
        repository.save(product);
    }

    @Transactional(readOnly = true)
    public BookedProductsDto checkAvailability(ShoppingCartDto cart) {
        Map<UUID, WarehouseProduct> productsMap = repository
                .findAllById(cart.getProducts().keySet())
                .stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, p -> p));

        double totalWeight = 0;
        double totalVolume = 0;
        boolean hasFragile = false;

        for (Map.Entry<UUID, Long> entry : cart.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            long requestedQty = entry.getValue();

            WarehouseProduct product = productsMap.get(productId);
            if (product == null) {
                throw new NoSpecifiedProductInWarehouseException(
                        "Product not found in warehouse: " + productId);
            }

            if (product.getQuantity() < requestedQty) {
                throw new ProductInShoppingCartLowQuantityInWarehouse(
                        "Not enough stock for product: " + productId);
            }

            totalWeight += product.getWeight() * requestedQty;
            totalVolume += product.getWidth() * product.getHeight() * product.getDepth() * requestedQty;
            if (product.isFragile()) {
                hasFragile = true;
            }
        }

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
    }

    public AddressDto getAddress() {
        return AddressDto.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS)
                .build();
    }

    @Transactional
    public BookedProductsDto assemblyProductsForOrder(AssemblyProductsForOrderRequest request) {
        Map<UUID, WarehouseProduct> productsMap = repository
                .findAllById(request.getProducts().keySet())
                .stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, p -> p));

        double totalWeight = 0;
        double totalVolume = 0;
        boolean hasFragile = false;
        List<WarehouseProduct> toPersist = new ArrayList<>();

        for (Map.Entry<UUID, Long> entry : request.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            long requestedQty = entry.getValue();

            WarehouseProduct product = productsMap.get(productId);
            if (product == null) {
                throw new NoSpecifiedProductInWarehouseException(
                        "Product not found in warehouse: " + productId);
            }

            if (product.getQuantity() < requestedQty) {
                throw new ProductInShoppingCartLowQuantityInWarehouse(
                        "Not enough stock for product: " + productId);
            }

            product.setQuantity(product.getQuantity() - requestedQty);
            toPersist.add(product);

            totalWeight += product.getWeight() * requestedQty;
            totalVolume += product.getWidth() * product.getHeight() * product.getDepth() * requestedQty;
            if (product.isFragile()) {
                hasFragile = true;
            }
        }

        repository.saveAll(toPersist);

        OrderBooking booking = OrderBooking.builder()
                .orderId(request.getOrderId())
                .products(request.getProducts())
                .build();
        orderBookingRepository.save(booking);

        return BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();
    }

    @Transactional
    public void shippedToDelivery(ShippedToDeliveryRequest request) {
        OrderBooking booking = orderBookingRepository.findById(request.getOrderId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Order booking not found: " + request.getOrderId()));
        booking.setDeliveryId(request.getDeliveryId());
        orderBookingRepository.save(booking);
    }

    @Transactional
    public void acceptReturn(Map<UUID, Long> products) {
        Map<UUID, WarehouseProduct> productsMap = repository
                .findAllById(products.keySet())
                .stream()
                .collect(Collectors.toMap(WarehouseProduct::getProductId, p -> p));

        List<WarehouseProduct> toPersist = new ArrayList<>();
        for (Map.Entry<UUID, Long> entry : products.entrySet()) {
            WarehouseProduct product = productsMap.get(entry.getKey());
            if (product == null) {
                throw new NoSpecifiedProductInWarehouseException(
                        "Product not found in warehouse: " + entry.getKey());
            }
            product.setQuantity(product.getQuantity() + entry.getValue());
            toPersist.add(product);
        }
        repository.saveAll(toPersist);
    }

    private WarehouseProduct findProduct(UUID productId) {
        return repository.findById(productId)
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseException(
                        "Product not found in warehouse: " + productId));
    }
}

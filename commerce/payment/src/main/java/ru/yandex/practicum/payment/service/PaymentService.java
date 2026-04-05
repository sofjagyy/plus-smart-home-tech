package ru.yandex.practicum.payment.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.interaction.api.client.OrderClient;
import ru.yandex.practicum.interaction.api.client.ShoppingStoreClient;
import ru.yandex.practicum.interaction.api.dto.OrderDto;
import ru.yandex.practicum.interaction.api.dto.PaymentDto;
import ru.yandex.practicum.interaction.api.dto.ProductDto;
import ru.yandex.practicum.interaction.api.enums.PaymentState;
import ru.yandex.practicum.interaction.api.exception.NoOrderFoundException;
import ru.yandex.practicum.interaction.api.exception.NotEnoughInfoInOrderToCalculateException;
import ru.yandex.practicum.payment.entity.Payment;
import ru.yandex.practicum.payment.repository.PaymentRepository;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final BigDecimal VAT_RATE = new BigDecimal("0.10");

    private final PaymentRepository paymentRepository;
    private final OrderClient orderClient;
    private final ShoppingStoreClient shoppingStoreClient;

    public BigDecimal productCost(OrderDto order) {
        if (order.getProducts() == null || order.getProducts().isEmpty()) {
            throw new NotEnoughInfoInOrderToCalculateException("Order has no products");
        }

        BigDecimal total = BigDecimal.ZERO;
        for (Map.Entry<UUID, Long> entry : order.getProducts().entrySet()) {
            ProductDto product = shoppingStoreClient.getProduct(entry.getKey());
            total = total.add(product.getPrice().multiply(BigDecimal.valueOf(entry.getValue())));
        }
        return total;
    }

    public BigDecimal getTotalCost(OrderDto order) {
        if (order.getProductPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateException("Product price is not calculated");
        }

        BigDecimal productPrice = order.getProductPrice();
        BigDecimal fee = productPrice.multiply(VAT_RATE);
        BigDecimal deliveryPrice = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : BigDecimal.ZERO;

        return productPrice.add(fee).add(deliveryPrice);
    }

    @Transactional
    public PaymentDto payment(OrderDto order) {
        BigDecimal productPrice = order.getProductPrice() != null ? order.getProductPrice() : BigDecimal.ZERO;
        BigDecimal deliveryPrice = order.getDeliveryPrice() != null ? order.getDeliveryPrice() : BigDecimal.ZERO;
        BigDecimal fee = productPrice.multiply(VAT_RATE);
        BigDecimal totalPayment = productPrice.add(fee).add(deliveryPrice);

        Payment payment = Payment.builder()
                .totalPayment(totalPayment)
                .deliveryTotal(deliveryPrice)
                .feeTotal(fee)
                .paymentState(PaymentState.PENDING)
                .orderId(order.getOrderId())
                .build();

        payment = paymentRepository.save(payment);

        return PaymentDto.builder()
                .paymentId(payment.getPaymentId())
                .totalPayment(payment.getTotalPayment())
                .deliveryTotal(payment.getDeliveryTotal())
                .feeTotal(payment.getFeeTotal())
                .build();
    }

    @Transactional
    public void paymentSuccess(UUID paymentId) {
        Payment payment = findPayment(paymentId);
        payment.setPaymentState(PaymentState.SUCCESS);
        paymentRepository.save(payment);
        orderClient.payment(payment.getOrderId());
    }

    @Transactional
    public void paymentFailed(UUID paymentId) {
        Payment payment = findPayment(paymentId);
        payment.setPaymentState(PaymentState.FAILED);
        paymentRepository.save(payment);
        orderClient.paymentFailed(payment.getOrderId());
    }

    private Payment findPayment(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundException("Payment not found: " + paymentId));
    }
}

package ru.yandex.practicum.delivery;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import ru.yandex.practicum.interaction.api.client.OrderClient;
import ru.yandex.practicum.interaction.api.client.WarehouseClient;

@SpringBootApplication
@EnableFeignClients(clients = {OrderClient.class, WarehouseClient.class})
public class DeliveryApplication {
    public static void main(String[] args) {
        SpringApplication.run(DeliveryApplication.class, args);
    }
}

package ru.yandex.practicum.aggregator;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {

    private static final String SENSOR_TOPIC = "telemetry.sensors.v1";
    private static final String SNAPSHOT_TOPIC = "telemetry.snapshots.v1";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaConsumer<String, SensorEventAvro> consumer;
    private final KafkaProducer<String, SpecificRecordBase> producer;
    private final SnapshotService snapshotService;

    public void start() {
        try {
            consumer.subscribe(List.of(SENSOR_TOPIC));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    Optional<SensorsSnapshotAvro> snapshot = snapshotService.updateState(event);

                    if (snapshot.isPresent()) {
                        producer.send(new ProducerRecord<>(
                                SNAPSHOT_TOPIC,
                                snapshot.get().getHubId(),
                                snapshot.get()
                        ));
                    }
                }

                consumer.commitSync();
            }

        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync();
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }
}

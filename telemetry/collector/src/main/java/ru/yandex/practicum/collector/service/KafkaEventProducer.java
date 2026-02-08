package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.model.hub.*;
import ru.yandex.practicum.collector.model.sensor.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaEventProducer {

    private static final String SENSOR_TOPIC = "telemetry.sensors.v1";
    private static final String HUB_TOPIC = "telemetry.hubs.v1";

    private final KafkaProducer<String, SpecificRecordBase> kafkaProducer;

    public void sendSensorEvent(SensorEvent event) {
        SensorEventAvro avro = toSensorEventAvro(event);
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(SENSOR_TOPIC, event.getHubId(), avro);
        kafkaProducer.send(record);
        log.info("Sensor event sent: type={}, id={}, hubId={}", event.getType(), event.getId(), event.getHubId());
    }

    public void sendHubEvent(HubEvent event) {
        HubEventAvro avro = toHubEventAvro(event);
        ProducerRecord<String, SpecificRecordBase> record =
                new ProducerRecord<>(HUB_TOPIC, event.getHubId(), avro);
        kafkaProducer.send(record);
        log.info("Hub event sent: type={}, hubId={}", event.getType(), event.getHubId());
    }

    private SensorEventAvro toSensorEventAvro(SensorEvent event) {
        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(toSensorPayload(event))
                .build();
    }

    private Object toSensorPayload(SensorEvent event) {
        return switch (event.getType()) {
            case CLIMATE_SENSOR_EVENT -> {
                ClimateSensorEvent e = (ClimateSensorEvent) event;
                yield ClimateSensorAvro.newBuilder()
                        .setTemperatureC(e.getTemperatureC())
                        .setHumidity(e.getHumidity())
                        .setCo2Level(e.getCo2Level())
                        .build();
            }
            case LIGHT_SENSOR_EVENT -> {
                LightSensorEvent e = (LightSensorEvent) event;
                yield LightSensorAvro.newBuilder()
                        .setLinkQuality(e.getLinkQuality())
                        .setLuminosity(e.getLuminosity())
                        .build();
            }
            case MOTION_SENSOR_EVENT -> {
                MotionSensorEvent e = (MotionSensorEvent) event;
                yield MotionSensorAvro.newBuilder()
                        .setLinkQuality(e.getLinkQuality())
                        .setMotion(e.isMotion())
                        .setVoltage(e.getVoltage())
                        .build();
            }
            case SWITCH_SENSOR_EVENT -> {
                SwitchSensorEvent e = (SwitchSensorEvent) event;
                yield SwitchSensorAvro.newBuilder()
                        .setState(e.isState())
                        .build();
            }
            case TEMPERATURE_SENSOR_EVENT -> {
                TemperatureSensorEvent e = (TemperatureSensorEvent) event;
                yield TemperatureSensorAvro.newBuilder()
                        .setId(e.getId())
                        .setHubId(e.getHubId())
                        .setTimestamp(e.getTimestamp())
                        .setTemperatureC(e.getTemperatureC())
                        .setTemperatureF(e.getTemperatureF())
                        .build();
            }
        };
    }

    private HubEventAvro toHubEventAvro(HubEvent event) {
        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(toHubPayload(event))
                .build();
    }

    private Object toHubPayload(HubEvent event) {
        return switch (event.getType()) {
            case DEVICE_ADDED -> {
                DeviceAddedEvent e = (DeviceAddedEvent) event;
                yield DeviceAddedEventAvro.newBuilder()
                        .setId(e.getId())
                        .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                        .build();
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEvent e = (DeviceRemovedEvent) event;
                yield DeviceRemovedEventAvro.newBuilder()
                        .setId(e.getId())
                        .build();
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEvent e = (ScenarioAddedEvent) event;
                yield ScenarioAddedEventAvro.newBuilder()
                        .setName(e.getName())
                        .setConditions(e.getConditions().stream()
                                .map(c -> ScenarioConditionAvro.newBuilder()
                                        .setSensorId(c.getSensorId())
                                        .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                        .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                        .setValue(c.getValue())
                                        .build())
                                .collect(Collectors.toList()))
                        .setActions(e.getActions().stream()
                                .map(a -> DeviceActionAvro.newBuilder()
                                        .setSensorId(a.getSensorId())
                                        .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                        .setValue(a.getValue())
                                        .build())
                                .collect(Collectors.toList()))
                        .build();
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEvent e = (ScenarioRemovedEvent) event;
                yield ScenarioRemovedEventAvro.newBuilder()
                        .setName(e.getName())
                        .build();
            }
        };
    }

}

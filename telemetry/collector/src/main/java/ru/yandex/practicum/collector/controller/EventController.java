package ru.yandex.practicum.collector.controller;

import com.google.protobuf.Empty;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.stream.Collectors;

@GrpcService
@RequiredArgsConstructor
@Slf4j
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {

    private static final String SENSOR_TOPIC = "telemetry.sensors.v1";
    private static final String HUB_TOPIC = "telemetry.hubs.v1";

    private final KafkaProducer<String, SpecificRecordBase> kafkaProducer;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            SensorEventAvro avro = toSensorEventAvro(request);
            ProducerRecord<String, SpecificRecordBase> record =
                    new ProducerRecord<>(SENSOR_TOPIC, request.getHubId(), avro);
            kafkaProducer.send(record);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            HubEventAvro avro = toHubEventAvro(request);
            ProducerRecord<String, SpecificRecordBase> record =
                    new ProducerRecord<>(HUB_TOPIC, request.getHubId(), avro);
            kafkaProducer.send(record);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    private SensorEventAvro toSensorEventAvro(SensorEventProto proto) {
        return SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(toInstant(proto.getTimestamp()))
                .setPayload(toSensorPayload(proto))
                .build();
    }

    private Object toSensorPayload(SensorEventProto proto) {
        return switch (proto.getPayloadCase()) {
            case CLIMATE_SENSOR -> {
                ClimateSensorProto p = proto.getClimateSensor();
                yield ClimateSensorAvro.newBuilder()
                        .setTemperatureC(p.getTemperatureC())
                        .setHumidity(p.getHumidity())
                        .setCo2Level(p.getCo2Level())
                        .build();
            }
            case LIGHT_SENSOR -> {
                LightSensorProto p = proto.getLightSensor();
                yield LightSensorAvro.newBuilder()
                        .setLinkQuality(p.getLinkQuality())
                        .setLuminosity(p.getLuminosity())
                        .build();
            }
            case MOTION_SENSOR -> {
                MotionSensorProto p = proto.getMotionSensor();
                yield MotionSensorAvro.newBuilder()
                        .setLinkQuality(p.getLinkQuality())
                        .setMotion(p.getMotion())
                        .setVoltage(p.getVoltage())
                        .build();
            }
            case SWITCH_SENSOR -> {
                SwitchSensorProto p = proto.getSwitchSensor();
                yield SwitchSensorAvro.newBuilder()
                        .setState(p.getState())
                        .build();
            }
            case TEMPERATURE_SENSOR -> {
                TemperatureSensorProto p = proto.getTemperatureSensor();
                yield TemperatureSensorAvro.newBuilder()
                        .setId(proto.getId())
                        .setHubId(proto.getHubId())
                        .setTimestamp(toInstant(proto.getTimestamp()))
                        .setTemperatureC(p.getTemperatureC())
                        .setTemperatureF(p.getTemperatureF())
                        .build();
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Sensor payload not set");
        };
    }

    private HubEventAvro toHubEventAvro(HubEventProto proto) {
        return HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(toInstant(proto.getTimestamp()))
                .setPayload(toHubPayload(proto))
                .build();
    }

    private Object toHubPayload(HubEventProto proto) {
        return switch (proto.getPayloadCase()) {
            case DEVICE_ADDED -> {
                DeviceAddedEventProto p = proto.getDeviceAdded();
                yield DeviceAddedEventAvro.newBuilder()
                        .setId(p.getId())
                        .setType(DeviceTypeAvro.valueOf(p.getType().name()))
                        .build();
            }
            case DEVICE_REMOVED -> {
                DeviceRemovedEventProto p = proto.getDeviceRemoved();
                yield DeviceRemovedEventAvro.newBuilder()
                        .setId(p.getId())
                        .build();
            }
            case SCENARIO_ADDED -> {
                ScenarioAddedEventProto p = proto.getScenarioAdded();
                yield ScenarioAddedEventAvro.newBuilder()
                        .setName(p.getName())
                        .setConditions(p.getConditionList().stream()
                                .map(c -> ScenarioConditionAvro.newBuilder()
                                        .setSensorId(c.getSensorId())
                                        .setType(ConditionTypeAvro.valueOf(c.getType().name()))
                                        .setOperation(ConditionOperationAvro.valueOf(c.getOperation().name()))
                                        .setValue(toConditionValue(c))
                                        .build())
                                .collect(Collectors.toList()))
                        .setActions(p.getActionList().stream()
                                .map(a -> DeviceActionAvro.newBuilder()
                                        .setSensorId(a.getSensorId())
                                        .setType(ActionTypeAvro.valueOf(a.getType().name()))
                                        .setValue(a.hasValue() ? a.getValue() : null)
                                        .build())
                                .collect(Collectors.toList()))
                        .build();
            }
            case SCENARIO_REMOVED -> {
                ScenarioRemovedEventProto p = proto.getScenarioRemoved();
                yield ScenarioRemovedEventAvro.newBuilder()
                        .setName(p.getName())
                        .build();
            }
            case PAYLOAD_NOT_SET -> throw new IllegalArgumentException("Hub payload not set");
        };
    }

    private Object toConditionValue(ScenarioConditionProto condition) {
        return switch (condition.getValueCase()) {
            case BOOL_VALUE -> condition.getBoolValue();
            case INT_VALUE -> condition.getIntValue();
            case VALUE_NOT_SET -> null;
        };
    }

    private Instant toInstant(Timestamp timestamp) {
        return Instant.ofEpochSecond(timestamp.getSeconds(), timestamp.getNanos());
    }
}

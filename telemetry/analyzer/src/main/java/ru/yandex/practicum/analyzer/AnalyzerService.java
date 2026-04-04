package ru.yandex.practicum.analyzer;

import com.google.protobuf.Timestamp;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.entity.Scenario;
import ru.yandex.practicum.analyzer.entity.ScenarioAction;
import ru.yandex.practicum.analyzer.entity.ScenarioCondition;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class AnalyzerService {

    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public AnalyzerService(ScenarioRepository scenarioRepository,
                           @GrpcClient("hub-router")
                           HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    @PostConstruct
    public void warmUp() {
        scenarioRepository.findByHubId("__warmup__");
    }

    @Transactional(readOnly = true)
    public void analyze(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);

        for (Scenario scenario : scenarios) {
            boolean allConditionsMet = scenario.getConditions().stream()
                    .allMatch(sc -> checkCondition(sc, sensorsState));

            if (allConditionsMet && !scenario.getConditions().isEmpty()) {
                executeActions(scenario, snapshot.getTimestamp());
            }
        }
    }

    private boolean checkCondition(ScenarioCondition sc, Map<String, SensorStateAvro> sensorsState) {
        String sensorId = sc.getSensor().getId();
        SensorStateAvro state = sensorsState.get(sensorId);

        if (state == null) {
            return false;
        }

        Integer sensorValue = extractSensorValue(state.getData(), sc.getCondition().getType());
        if (sensorValue == null) {
            return false;
        }

        Integer conditionValue = sc.getCondition().getValue();
        if (conditionValue == null) {
            return false;
        }

        return switch (sc.getCondition().getOperation()) {
            case "EQUALS" -> sensorValue.equals(conditionValue);
            case "GREATER_THAN" -> sensorValue > conditionValue;
            case "LOWER_THAN" -> sensorValue < conditionValue;
            default -> false;
        };
    }

    private Integer extractSensorValue(Object data, String conditionType) {
        return switch (conditionType) {
            case "MOTION" -> {
                if (data instanceof MotionSensorAvro m) yield m.getMotion() ? 1 : 0;
                yield null;
            }
            case "LUMINOSITY" -> {
                if (data instanceof LightSensorAvro l) yield l.getLuminosity();
                yield null;
            }
            case "SWITCH" -> {
                if (data instanceof SwitchSensorAvro s) yield s.getState() ? 1 : 0;
                yield null;
            }
            case "TEMPERATURE" -> {
                if (data instanceof TemperatureSensorAvro t) yield t.getTemperatureC();
                if (data instanceof ClimateSensorAvro c) yield c.getTemperatureC();
                yield null;
            }
            case "CO2LEVEL" -> {
                if (data instanceof ClimateSensorAvro c) yield c.getCo2Level();
                yield null;
            }
            case "HUMIDITY" -> {
                if (data instanceof ClimateSensorAvro c) yield c.getHumidity();
                yield null;
            }
            default -> null;
        };
    }

    private void executeActions(Scenario scenario, Instant snapshotTimestamp) {
        Timestamp ts = Timestamp.newBuilder()
                .setSeconds(snapshotTimestamp.getEpochSecond())
                .setNanos(snapshotTimestamp.getNano())
                .build();

        for (ScenarioAction sa : scenario.getActions()) {
            DeviceActionProto.Builder actionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(sa.getSensor().getId())
                    .setType(ActionTypeProto.valueOf(sa.getAction().getType()));

            if (sa.getAction().getValue() != null) {
                actionBuilder.setValue(sa.getAction().getValue());
            }

            DeviceActionRequest request = DeviceActionRequest.newBuilder()
                    .setHubId(scenario.getHubId())
                    .setScenarioName(scenario.getName())
                    .setAction(actionBuilder.build())
                    .setTimestamp(ts)
                    .build();

            try {
                hubRouterClient.handleDeviceAction(request);
            } catch (Exception e) {
                log.error("Ошибка при отправке действия для сценария {}", scenario.getName(), e);
            }
        }
    }
}

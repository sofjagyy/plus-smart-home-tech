package ru.yandex.practicum.analyzer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.entity.*;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {

    private static final String HUB_TOPIC = "telemetry.hubs.v1";
    private static final Duration POLL_TIMEOUT = Duration.ofMillis(500);

    private final KafkaConsumer<String, HubEventAvro> hubEventConsumer;
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Override
    public void run() {
        try {
            hubEventConsumer.subscribe(List.of(HUB_TOPIC));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = hubEventConsumer.poll(POLL_TIMEOUT);

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    String hubId = event.getHubId();
                    Object payload = event.getPayload();

                    if (payload instanceof DeviceAddedEventAvro added) {
                        handleDeviceAdded(hubId, added);
                    } else if (payload instanceof DeviceRemovedEventAvro removed) {
                        handleDeviceRemoved(hubId, removed);
                    } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
                        handleScenarioAdded(hubId, scenarioAdded);
                    } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
                        handleScenarioRemoved(hubId, scenarioRemoved);
                    }
                }
            }

        } catch (WakeupException ignored) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хаба", e);
        } finally {
            try {
                hubEventConsumer.commitSync();
            } finally {
                log.info("Закрываем hubEventConsumer");
                hubEventConsumer.close();
            }
        }
    }

    private void handleDeviceAdded(String hubId, DeviceAddedEventAvro event) {
        if (sensorRepository.findById(event.getId()).isPresent()) {
            return;
        }
        Sensor sensor = new Sensor();
        sensor.setId(event.getId());
        sensor.setHubId(hubId);
        sensorRepository.save(sensor);
    }

    private void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro event) {
        sensorRepository.findByIdAndHubId(event.getId(), hubId)
                .ifPresent(sensorRepository::delete);
    }

    private void handleScenarioAdded(String hubId, ScenarioAddedEventAvro event) {
        Optional<Scenario> existing = scenarioRepository.findByHubIdAndName(hubId, event.getName());
        if (existing.isPresent()) {
            return;
        }

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(event.getName());
        scenario.setConditions(new HashSet<>());
        scenario.setActions(new HashSet<>());

        scenario = scenarioRepository.save(scenario);

        for (ScenarioConditionAvro c : event.getConditions()) {
            Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(c.getSensorId(), hubId);
            if (sensor.isEmpty()) continue;

            Condition condition = new Condition();
            condition.setType(c.getType().name());
            condition.setOperation(c.getOperation().name());
            if (c.getValue() instanceof Integer intVal) {
                condition.setValue(intVal);
            } else if (c.getValue() instanceof Boolean boolVal) {
                condition.setValue(boolVal ? 1 : 0);
            }
            condition = conditionRepository.save(condition);

            ScenarioCondition sc = new ScenarioCondition();
            sc.setScenario(scenario);
            sc.setSensor(sensor.get());
            sc.setCondition(condition);
            scenario.getConditions().add(sc);
        }

        for (DeviceActionAvro a : event.getActions()) {
            Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(a.getSensorId(), hubId);
            if (sensor.isEmpty()) continue;

            Action action = new Action();
            action.setType(a.getType().name());
            action.setValue(a.getValue());
            action = actionRepository.save(action);

            ScenarioAction sa = new ScenarioAction();
            sa.setScenario(scenario);
            sa.setSensor(sensor.get());
            sa.setAction(action);
            scenario.getActions().add(sa);
        }

        scenarioRepository.save(scenario);
    }

    private void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro event) {
        scenarioRepository.findByHubIdAndName(hubId, event.getName())
                .ifPresent(scenarioRepository::delete);
    }
}

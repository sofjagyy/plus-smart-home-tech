package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Entity
@Table(name = "scenario_actions")
@IdClass(ScenarioActionId.class)
@Getter
@Setter
@ToString(exclude = {"scenario"})
public class ScenarioAction {

    @Id
    @ManyToOne
    @JoinColumn(name = "scenario_id")
    private Scenario scenario;

    @Id
    @ManyToOne
    @JoinColumn(name = "sensor_id")
    private Sensor sensor;

    @Id
    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "action_id")
    private Action action;
}

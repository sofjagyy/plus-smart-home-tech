package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Entity
@Table(name = "scenario_conditions")
@IdClass(ScenarioConditionId.class)
@Getter
@Setter
@ToString(exclude = {"scenario"})
public class ScenarioCondition {

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
    @JoinColumn(name = "condition_id")
    private Condition condition;
}

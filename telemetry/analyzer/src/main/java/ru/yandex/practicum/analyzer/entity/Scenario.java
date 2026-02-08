package ru.yandex.practicum.analyzer.entity;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Set;

@Entity
@Table(name = "scenarios")
@Getter
@Setter
@ToString(exclude = {"conditions", "actions"})
public class Scenario {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String hubId;

    private String name;

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private Set<ScenarioCondition> conditions;

    @OneToMany(mappedBy = "scenario", cascade = CascadeType.ALL, orphanRemoval = true, fetch = FetchType.EAGER)
    private Set<ScenarioAction> actions;
}

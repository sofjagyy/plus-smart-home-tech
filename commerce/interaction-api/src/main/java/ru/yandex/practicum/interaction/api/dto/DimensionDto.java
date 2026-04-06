package ru.yandex.practicum.interaction.api.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.NoArgsConstructor;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DimensionDto {
    private double width;
    private double height;
    private double depth;
}

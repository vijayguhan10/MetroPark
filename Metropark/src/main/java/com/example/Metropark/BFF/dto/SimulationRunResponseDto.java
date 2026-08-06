package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public record SimulationRunResponseDto(
        @JsonProperty("status") String status,
        @JsonProperty("states") List<SimulationStateDto> states,
        @JsonProperty("events") List<SimulationEventDto> events
) {
}

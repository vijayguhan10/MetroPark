package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SimulationStateDto(
        @JsonProperty("state") String state,
        @JsonProperty("service") String service,
        @JsonProperty("result") String result
) {
}

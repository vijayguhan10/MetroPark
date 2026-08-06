package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record SimulationEventDto(
    @JsonProperty("type") String type,
    @JsonProperty("message") String message,
    @JsonProperty("sessionId") Integer sessionId,
    @JsonProperty("timestamp") LocalDateTime timestamp
) {}
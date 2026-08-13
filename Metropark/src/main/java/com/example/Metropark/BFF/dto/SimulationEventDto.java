package com.example.Metropark.BFF.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SimulationEventDto(
    @JsonProperty("type") String type,
    @JsonProperty("message") String message,
    @JsonProperty("sessionId") Integer sessionId,
    @JsonProperty("timestamp") LocalDateTime timestamp
) {}
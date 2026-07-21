package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record ActiveSessionDto(
    @JsonProperty("sessionId") String sessionId,
    @JsonProperty("plate") String plate,
    @JsonProperty("vehicle") String vehicle,
    @JsonProperty("slot") String slot,
    @JsonProperty("status") String status,
    @JsonProperty("entryTime") LocalDateTime entryTime,
    @JsonProperty("durationMinutes") Integer durationMinutes
) {}
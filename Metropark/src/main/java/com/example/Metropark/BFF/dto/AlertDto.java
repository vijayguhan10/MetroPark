package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record AlertDto(
    @JsonProperty("alertId") String alertId,
    @JsonProperty("type") String type,
    @JsonProperty("severity") String severity,
    @JsonProperty("message") String message,
    @JsonProperty("locationId") String locationId,
    @JsonProperty("createdAt") LocalDateTime createdAt,
    @JsonProperty("acknowledged") boolean acknowledged
) {}
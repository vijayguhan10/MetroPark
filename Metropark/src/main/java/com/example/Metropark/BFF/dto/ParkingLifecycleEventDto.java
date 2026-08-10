package com.example.Metropark.BFF.dto;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ParkingLifecycleEventDto(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("type") String type,
        @JsonProperty("entityId") String entityId,
        @JsonProperty("version") long version,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("payload") Object payload
) {}

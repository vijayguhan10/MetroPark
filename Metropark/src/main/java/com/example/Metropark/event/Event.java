package com.example.Metropark.event;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

public record Event(
        @JsonProperty("eventId") String eventId,
        @JsonProperty("type") String type,
        @JsonProperty("entityId") String entityId,
        @JsonProperty("version") long version,
        @JsonProperty("timestamp") Instant timestamp,
        @JsonProperty("payload") Object payload
) {
    public static <T> Event of(String type, String entityId, long version, T payload) {
        return new Event(
                UUID.randomUUID().toString(),
                type,
                entityId,
                version,
                Instant.now(),
                payload
        );
    }
}
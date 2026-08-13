package com.example.Metropark.event.payload;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SlotEventPayload(
        @JsonProperty("slotId") Integer slotId,
        @JsonProperty("locationId") String locationId,
        @JsonProperty("displayCode") String displayCode,
        @JsonProperty("vehicleTypeId") Integer vehicleTypeId,
        @JsonProperty("reservationClassId") Integer reservationClassId,
        @JsonProperty("sensorId") String sensorId,
        @JsonProperty("currentStatus") String currentStatus,
        @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}

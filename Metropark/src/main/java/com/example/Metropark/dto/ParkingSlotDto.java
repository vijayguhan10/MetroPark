package com.example.Metropark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ParkingSlotDto(
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("locationId") String locationId,
    @JsonProperty("displayCode") String displayCode,
    @JsonProperty("vehicleTypeId") Integer vehicleTypeId,
    @JsonProperty("reservationClassId") Integer reservationClassId,
    @JsonProperty("sensorId") String sensorId,
    @JsonProperty("currentStatus") String currentStatus
    // @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
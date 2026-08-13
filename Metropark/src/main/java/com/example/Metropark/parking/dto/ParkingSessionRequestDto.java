package com.example.Metropark.parking.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record ParkingSessionRequestDto(
    @JsonProperty("vehicleId") Integer vehicleId,
    @JsonProperty("userId") String userId,
    @JsonProperty("locationId") String locationId,
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("fromDate") LocalDateTime fromDate,
    @JsonProperty("toDate") LocalDateTime toDate
) {}
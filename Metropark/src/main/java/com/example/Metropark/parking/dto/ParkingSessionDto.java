package com.example.Metropark.parking.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record ParkingSessionDto(
    @JsonProperty("sessionId") Integer sessionId,
    @JsonProperty("reservationId") Integer reservationId,
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("userId") String userId,
    @JsonProperty("vehicleId") Integer vehicleId,
    @JsonProperty("entryGateId") Integer entryGateId,
    @JsonProperty("exitGateId") Integer exitGateId,
    @JsonProperty("sessionStatus") String sessionStatus,
    @JsonProperty("actualEntryTime") LocalDateTime actualEntryTime,
    @JsonProperty("actualExitTime") LocalDateTime actualExitTime,
    @JsonProperty("expectedExitTime") LocalDateTime expectedExitTime,
    @JsonProperty("durationMinutes") Integer durationMinutes,
    @JsonProperty("paymentStatus") String paymentStatus,
    @JsonProperty("sessionVersion") Integer sessionVersion,
    @JsonProperty("createdAt") LocalDateTime createdAt,
    @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
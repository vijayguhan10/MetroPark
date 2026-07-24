package com.example.Metropark.parking.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ParkingSessionResponseDto(
    @JsonProperty("sessionId") Integer sessionId,
    @JsonProperty("reservationId") Integer reservationId,
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("slotDisplayCode") String slotDisplayCode, // From parking_slots
    @JsonProperty("locationId") String locationId, // From parking_slots
    @JsonProperty("userId") String userId,
    @JsonProperty("userName") String userName, // From users
    @JsonProperty("vehicleId") Integer vehicleId,
    @JsonProperty("vehicleNumber") String vehicleNumber, // From vehicles
    @JsonProperty("entryGateId") Integer entryGateId,
    @JsonProperty("entryGateName") String entryGateName, // From gates
    @JsonProperty("exitGateId") Integer exitGateId,
    @JsonProperty("exitGateName") String exitGateName, // From gates
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
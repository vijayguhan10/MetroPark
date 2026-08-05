package com.example.Metropark.reservation.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReservationDto(
    @JsonProperty("reservationId") Integer reservationId,
    @JsonProperty("userId") String userId,
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("queueEntryId") Integer queueEntryId,
    @JsonProperty("reservationStatus") String reservationStatus,
    @JsonProperty("reservationVersion") Integer reservationVersion,
    @JsonProperty("reservedAt") LocalDateTime reservedAt,
    @JsonProperty("expiresAt") LocalDateTime expiresAt,
    @JsonProperty("createdAt") LocalDateTime createdAt,
    @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
package com.example.Metropark.reservation.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record ReservationDto(
    @JsonProperty("reservationId") Integer reservationId,
    @JsonProperty("userId") Integer userId,
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("queueEntryId") Integer queueEntryId,
    @JsonProperty("reservationStatus") String reservationStatus,
    @JsonProperty("reservationVersion") Integer reservationVersion,
    @JsonProperty("reservedAt") LocalDateTime reservedAt,
    @JsonProperty("expiresAt") LocalDateTime expiresAt,
    @JsonProperty("createdAt") LocalDateTime createdAt,
    @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
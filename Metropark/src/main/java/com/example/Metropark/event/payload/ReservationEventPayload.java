package com.example.Metropark.event.payload;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReservationEventPayload(
        @JsonProperty("reservationId") Integer reservationId,
        @JsonProperty("userId") String userId,
        @JsonProperty("slotId") Integer slotId,
        @JsonProperty("queueEntryId") Integer queueEntryId,
        @JsonProperty("reservationStatus") String reservationStatus,
        @JsonProperty("reservationVersion") Integer reservationVersion,
        @JsonProperty("reservedAt") LocalDateTime reservedAt,
        @JsonProperty("expiresAt") LocalDateTime expiresAt,
        @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}

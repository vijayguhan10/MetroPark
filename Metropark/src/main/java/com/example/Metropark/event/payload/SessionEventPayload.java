package com.example.Metropark.event.payload;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SessionEventPayload(
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
        @JsonProperty("surgeMultiplier") BigDecimal surgeMultiplier,
        @JsonProperty("sessionVersion") Integer sessionVersion,
        @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
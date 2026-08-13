package com.example.Metropark.event.payload;

import java.math.BigDecimal;
import com.fasterxml.jackson.annotation.JsonProperty;

public record NotificationEventPayload(
    @JsonProperty("eventId") String eventId,
    @JsonProperty("sessionId") Integer sessionId,
    @JsonProperty("userId") String userId,
    @JsonProperty("vehicleId") Integer vehicleId,
    @JsonProperty("durationMinutes") Integer durationMinutes,
    @JsonProperty("amount") BigDecimal amount,
    @JsonProperty("walletBalance") BigDecimal walletBalance,
    @JsonProperty("status") String status, // "SUCCESS" or "FAILED"
    @JsonProperty("userStatus") String userStatus, // "ACTIVE" or "SUSPENDED"
    @JsonProperty("message") String message
) {}

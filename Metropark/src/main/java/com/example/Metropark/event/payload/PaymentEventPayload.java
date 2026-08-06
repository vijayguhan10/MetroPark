package com.example.Metropark.event.payload;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PaymentEventPayload(
        @JsonProperty("paymentId") Long paymentId,
        @JsonProperty("transactionReference") String transactionReference,
        @JsonProperty("sessionId") Integer sessionId,
        @JsonProperty("userId") String userId,
        @JsonProperty("methodId") Integer methodId,
        @JsonProperty("amount") BigDecimal amount,
        @JsonProperty("currency") String currency,
        @JsonProperty("paymentStatus") String paymentStatus,
        @JsonProperty("gatewayResponseCode") String gatewayResponseCode,
        @JsonProperty("gatewayResponseMessage") String gatewayResponseMessage,
        @JsonProperty("processedAt") LocalDateTime processedAt,
        @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
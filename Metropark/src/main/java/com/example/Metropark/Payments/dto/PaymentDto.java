package com.example.Metropark.payments.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record PaymentDto(
                @JsonProperty("paymentId") Long paymentId,
                @JsonProperty("transactionReference") String transactionReference,
                @JsonProperty("sessionId") Integer sessionId,
                @JsonProperty("methodId") @NotNull Integer methodId,
                @JsonProperty("amount") @NotNull @Positive BigDecimal amount,
                @JsonProperty("currency") @NotNull @Size(min = 3, max = 3) String currency,
                @JsonProperty("paymentStatus") String paymentStatus,
                @JsonProperty("gatewayResponseCode") String gatewayResponseCode,
                @JsonProperty("gatewayResponseMessage") String gatewayResponseMessage,
                @JsonProperty("processedAt") LocalDateTime processedAt,
                @JsonProperty("createdAt") LocalDateTime createdAt,
                @JsonProperty("updatedAt") LocalDateTime updatedAt) {
}
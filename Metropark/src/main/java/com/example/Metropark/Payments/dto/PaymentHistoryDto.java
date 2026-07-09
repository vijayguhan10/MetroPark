package com.example.Metropark.payments.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record PaymentHistoryDto(
        @JsonProperty("historyId") Long historyId,
        @JsonProperty("paymentId") Long paymentId,
        @JsonProperty("previousStatus") String previousStatus,
        @JsonProperty("currentStatus") String currentStatus,
        @JsonProperty("changedBy") String changedBy,
        @JsonProperty("reason") String reason,
        @JsonProperty("gatewayReference") String gatewayReference,
        @JsonProperty("changedAt") LocalDateTime changedAt) {
}
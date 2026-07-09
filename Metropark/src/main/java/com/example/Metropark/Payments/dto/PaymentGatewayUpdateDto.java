package com.example.Metropark.payments.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

import java.time.LocalDateTime;

public record PaymentGatewayUpdateDto(
        @JsonProperty("transactionReference") String transactionReference,
        @JsonProperty("gatewayResponseCode") @NotBlank String gatewayResponseCode,
        @JsonProperty("gatewayResponseMessage") String gatewayResponseMessage,
        @JsonProperty("paymentStatus") @NotBlank String paymentStatus,
        @JsonProperty("changedBy") String changedBy,
        @JsonProperty("reason") String reason,
        @JsonProperty("processedAt") LocalDateTime processedAt) {
}
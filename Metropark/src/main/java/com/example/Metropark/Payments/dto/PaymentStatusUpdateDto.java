package com.example.Metropark.payments.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

public record PaymentStatusUpdateDto(
        @JsonProperty("status") @NotBlank String status,
        @JsonProperty("changedBy") @NotBlank String changedBy,
        @JsonProperty("reason") String reason,
        @JsonProperty("gatewayReference") String gatewayReference) {
}
package com.example.Metropark.payments.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;

import java.time.LocalDateTime;

public record PaymentMethodDto(
        @JsonProperty("methodId") Long methodId,
        @JsonProperty("methodName") @NotBlank String methodName,
        @JsonProperty("isActive") Boolean isActive,
        @JsonProperty("createdAt") LocalDateTime createdAt) {
}
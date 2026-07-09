package com.example.Metropark.payments.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;

import java.time.LocalDateTime;

public record ReservationBillingTypeDto(
        @JsonProperty("billingTypeId") Long billingTypeId,
        @JsonProperty("typeName") @NotBlank String typeName,
        @JsonProperty("durationValue") @NotNull @Positive Integer durationValue,
        @JsonProperty("durationUnit") @NotBlank String durationUnit,
        @JsonProperty("isSubscription") Boolean isSubscription,
        @JsonProperty("createdAt") LocalDateTime createdAt,
        @JsonProperty("updatedAt") LocalDateTime updatedAt) {
}
package com.example.Metropark.payments.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

public record PricingRateDto(
                @JsonProperty("rateId") Long rateId,
                @JsonProperty("locationId") @NotBlank String locationId,
                @JsonProperty("vehicleTypeId") @NotNull Integer vehicleTypeId,
                @JsonProperty("reservationClassId") @NotNull Integer reservationClassId,
                @JsonProperty("baseRate") @NotNull @Positive BigDecimal baseRate,
                @JsonProperty("currency") @NotBlank @Size(min = 3, max = 3) String currency,
                @JsonProperty("effectiveFrom") @NotNull LocalDateTime effectiveFrom,
                @JsonProperty("effectiveTo") LocalDateTime effectiveTo,
                @JsonProperty("isActive") Boolean isActive,
                @JsonProperty("createdAt") LocalDateTime createdAt,
                @JsonProperty("updatedAt") LocalDateTime updatedAt) {
}
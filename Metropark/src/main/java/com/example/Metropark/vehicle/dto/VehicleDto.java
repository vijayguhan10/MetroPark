package com.example.Metropark.vehicle.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record VehicleDto(
    @JsonProperty("vehicleId") Integer vehicleId,
    @JsonProperty("userId") String userId,
    @JsonProperty("vehicleNumber") String vehicleNumber,
    @JsonProperty("vehicleTypeId") Integer vehicleTypeId,
    @JsonProperty("brand") String brand,
    @JsonProperty("model") String model,
    @JsonProperty("color") String color,
    @JsonProperty("isActive") Boolean isActive,
    @JsonProperty("createdAt") LocalDateTime createdAt,
    @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
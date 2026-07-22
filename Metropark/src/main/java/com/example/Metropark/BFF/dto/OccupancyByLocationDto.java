package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record OccupancyByLocationDto(
    @JsonProperty("locationId") String locationId,
    @JsonProperty("locationName") String locationName,
    @JsonProperty("totalSlots") long totalSlots,
    @JsonProperty("occupiedSlots") long occupiedSlots,
    @JsonProperty("availableSlots") long availableSlots,
    @JsonProperty("occupancyPercentage") double occupancyPercentage
) {}
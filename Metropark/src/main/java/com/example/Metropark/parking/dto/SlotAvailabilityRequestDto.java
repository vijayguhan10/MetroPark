package com.example.Metropark.parking.dto;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SlotAvailabilityRequestDto(
    @JsonProperty("locationID") String locationId,
    @JsonProperty("fromDate") LocalDateTime fromDate,
    @JsonProperty("toDate") LocalDateTime toDate
) {}

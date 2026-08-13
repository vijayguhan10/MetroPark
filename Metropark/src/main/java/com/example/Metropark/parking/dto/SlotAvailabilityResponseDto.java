package com.example.Metropark.parking.dto;

import java.time.LocalDateTime;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public record SlotAvailabilityResponseDto(
    @JsonProperty("fromDate") LocalDateTime fromDate,
    @JsonProperty("toDate") LocalDateTime toDate,
    @JsonProperty("availableSlotIds") List<Integer> availableSlotIds,
    @JsonProperty("overlappingTimings") List<OverlappingTimingDto> overlappingTimings
) {
    public record OverlappingTimingDto(
        @JsonProperty("from") LocalDateTime from,
        @JsonProperty("to") LocalDateTime to
    ) {}
}

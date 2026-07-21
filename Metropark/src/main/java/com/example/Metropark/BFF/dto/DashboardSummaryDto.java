package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record DashboardSummaryDto(
    @JsonProperty("activeSessionsCount") long activeSessionsCount,
    @JsonProperty("occupancyPercentage") double occupancyPercentage,
    @JsonProperty("openAlertsCount") int openAlertsCount,
    @JsonProperty("totalSlots") long totalSlots,
    @JsonProperty("occupiedSlots") long occupiedSlots,
    @JsonProperty("availableSlots") long availableSlots
) {}
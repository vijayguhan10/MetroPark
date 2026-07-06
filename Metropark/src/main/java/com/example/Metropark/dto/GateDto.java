package com.example.Metropark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record GateDto(
    @JsonProperty("gateId") Integer gateId,
    @JsonProperty("locationId") String locationId,
    @JsonProperty("gateName") String gateName,
    @JsonProperty("gateType") String gateType,
    @JsonProperty("status") String status,
    @JsonProperty("createdAt") LocalDateTime createdAt,
    @JsonProperty("updatedAt") LocalDateTime updatedAt
) {}
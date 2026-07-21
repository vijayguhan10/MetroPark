package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record GateStatusDto(
    @JsonProperty("gateId") Integer gateId,
    @JsonProperty("gateName") String gateName,
    @JsonProperty("gateType") String gateType,
    @JsonProperty("status") String status,
    @JsonProperty("locationId") String locationId
) {}
package com.example.Metropark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record QueueDto(
    @JsonProperty("queueId") Integer queueId,
    @JsonProperty("locationId") String locationId,
    @JsonProperty("vehicleTypeId") Integer vehicleTypeId,
    @JsonProperty("reservationClassId") Integer reservationClassId,
    @JsonProperty("queueName") String queueName,
    @JsonProperty("status") String status,
    @JsonProperty("createdAt") LocalDateTime createdAt
) {}
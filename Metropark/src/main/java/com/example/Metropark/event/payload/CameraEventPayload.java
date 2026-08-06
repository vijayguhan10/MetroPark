package com.example.Metropark.event.payload;

import java.time.Instant;

import com.fasterxml.jackson.annotation.JsonProperty;

public record CameraEventPayload(
        @JsonProperty("licensePlate") String licensePlate,
        @JsonProperty("parkingLotId") String parkingLotId,
        @JsonProperty("cameraId") String cameraId,
        @JsonProperty("timestamp") Instant timestamp
) {}
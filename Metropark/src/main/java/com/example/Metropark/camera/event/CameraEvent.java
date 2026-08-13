package com.example.Metropark.camera.event;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public record CameraEvent(
        @JsonProperty("eventId") UUID eventId,
        @JsonProperty("eventType") CameraEventType eventType,
        @JsonProperty("licensePlate") String licensePlate,
        @JsonProperty("vehicleId") Integer vehicleId,
        @JsonProperty("userId") String userId,
        @JsonProperty("parkingLotId") String parkingLotId,
        @JsonProperty("cameraId") String cameraId,
        @JsonProperty("timestamp") Instant timestamp) {

    public static CameraEvent of(
            CameraEventType eventType,
            String licensePlate,
            Integer vehicleId,
            String userId,
            String parkingLotId,
            String cameraId) {

        return new CameraEvent(
                UUID.randomUUID(),
                eventType,
                licensePlate,
                vehicleId,
                userId,
                parkingLotId,
                cameraId,
                Instant.now());
    }

    @JsonIgnore
    public boolean isEntry() {
        return eventType == CameraEventType.CAR_ENTERED;
    }
}

package com.example.Metropark.camera.event;

import java.time.Instant;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One ANPR observation, exactly as it travels over RabbitMQ.
 *
 * <p>
 * Entry and exit are the same shape and differ only by {@link #eventType}, so a
 * single queue, a single table and a single audit path serve both. There are no
 * separate CameraEntryEvent / CameraExitEvent types: they would carry identical
 * fields and force every consumer to branch on class rather than on a value,
 * while the persisted row would need a discriminator column anyway.
 *
 * <p>
 * {@code vehicleId} and {@code userId} are what the simulator happens to know
 * because it fabricated the car. A real camera supplies only a plate, so both
 * are nullable and every consumer resolves what it needs from
 * {@link #licensePlate}. Trusting them would make the pipeline untestable
 * against real hardware.
 */
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

    /**
     * {@code @JsonIgnore} is load-bearing. Jackson treats an {@code isX()} method as
     * a bean property named "entry" and writes it into the JSON, but the record's
     * canonical constructor has no such component - so every published event failed
     * to deserialise with "Unrecognized field entry" and dead lettered on both
     * queues. Any derived accessor added here needs the same annotation.
     */
    @JsonIgnore
    public boolean isEntry() {
        return eventType == CameraEventType.CAR_ENTERED;
    }
}

package com.example.Metropark.event.generator;

import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.example.Metropark.event.EventPublisher;
import com.example.Metropark.event.payload.CameraEventPayload;

import reactor.core.publisher.Mono;

@Component
public class CameraEventGenerator {

    private static final Logger LOGGER = LoggerFactory.getLogger(CameraEventGenerator.class);

    private final EventPublisher eventPublisher;
    private final Random random = new Random();

    // Sample data for generating realistic events
    private static final List<String> LICENSE_PLATES = List.of(
            "MH12AB1234", "DL01CD5678", "KA03EF9012", "TN07GH3456", "GJ18IJ7890",
            "RJ14KL2345", "UP32MN6789", "WB26OP0123", "AP09QR4567", "KL08ST8901"
    );

    private static final List<String> PARKING_LOTS = List.of(
            "MALL_CENTRAL", "AIRPORT_T1", "AIRPORT_T2", "TECH_PARK_A", "TECH_PARK_B",
            "HOSPITAL_MAIN", "UNIVERSITY_CAMPUS", "STADIUM_NORTH", "STADIUM_SOUTH", "DOWNTOWN_PLAZA"
    );

    private static final List<String> CAMERAS = List.of(
            "CAM_ENTRANCE_01", "CAM_ENTRANCE_02", "CAM_EXIT_01", "CAM_EXIT_02",
            "CAM_LEVEL_1_A", "CAM_LEVEL_1_B", "CAM_LEVEL_2_A", "CAM_LEVEL_2_B",
            "CAM_LEVEL_3_A", "CAM_LEVEL_3_B"
    );

    private static final List<String> EVENT_TYPES = List.of("CAR_ENTERED", "CAR_EXITED");

    public CameraEventGenerator(EventPublisher eventPublisher) {
        this.eventPublisher = eventPublisher;
    }

    @Scheduled(fixedRate = 5000) // Every 5 seconds
    public void generateRandomCameraEvent() {
        try {
            String eventType = EVENT_TYPES.get(random.nextInt(EVENT_TYPES.size()));
            String licensePlate = LICENSE_PLATES.get(random.nextInt(LICENSE_PLATES.size()));
            String parkingLotId = PARKING_LOTS.get(random.nextInt(PARKING_LOTS.size()));
            String cameraId = CAMERAS.get(random.nextInt(CAMERAS.size()));
            Instant timestamp = Instant.now();

            CameraEventPayload payload = new CameraEventPayload(
                    licensePlate,
                    parkingLotId,
                    cameraId,
                    timestamp
            );

            if ("CAR_ENTERED".equals(eventType)) {
                eventPublisher.publishCameraCarEntered(payload)
                        .subscribe(
                                unused -> LOGGER.debug("Published CAR_ENTERED event: {} at {}", licensePlate, parkingLotId),
                                e -> LOGGER.error("Failed to publish CAR_ENTERED event", e)
                        );
            } else {
                eventPublisher.publishCameraCarExited(payload)
                        .subscribe(
                                unused -> LOGGER.debug("Published CAR_EXITED event: {} at {}", licensePlate, parkingLotId),
                                e -> LOGGER.error("Failed to publish CAR_EXITED event", e)
                        );
            }
        } catch (Exception e) {
            LOGGER.error("Error generating camera event", e);
        }
    }

    @Scheduled(fixedRate = 30000) // Every 30 seconds, generate burst of events
    public void generateBurstEvents() {
        int burstSize = random.nextInt(5) + 3; // 3-7 events
        for (int i = 0; i < burstSize; i++) {
            try {
                TimeUnit.MILLISECONDS.sleep(random.nextInt(500)); // Small delay between events
                generateRandomCameraEvent();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }
}
package com.example.Metropark.parking.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.event.CameraEventPublisher;
import com.example.Metropark.camera.event.CameraEventType;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSessionRequestDto;
import com.example.Metropark.parking.dto.ParkingSessionResponseDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.parking.service.ParkingSessionService;
import com.example.Metropark.parking.service.ParkingSlotService;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.service.VehicleService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@RestController
@RequestMapping("/api/parking-sessions")
public class ParkingSessionController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSessionController.class);

    private final ParkingSessionService service;
    private final VehicleService vehicleService;
    private final UserRepository userRepository;
    private final LocationRepository locationRepository;
    private final ParkingSlotRepository parkingSlotRepository;
    private final ParkingSlotService parkingSlotService;
    private final CameraEventPublisher cameraEventPublisher;

    public ParkingSessionController(
            ParkingSessionService service,
            VehicleService vehicleService,
            UserRepository userRepository,
            LocationRepository locationRepository,
            ParkingSlotRepository parkingSlotRepository,
            ParkingSlotService parkingSlotService,
            CameraEventPublisher cameraEventPublisher) {
        this.service = service;
        this.vehicleService = vehicleService;
        this.userRepository = userRepository;
        this.locationRepository = locationRepository;
        this.parkingSlotRepository = parkingSlotRepository;
        this.parkingSlotService = parkingSlotService;
        this.cameraEventPublisher = cameraEventPublisher;
    }

    @PostMapping
    public Mono<ResponseEntity<Void>> createParkingSession(@RequestBody ParkingSessionRequestDto request) {
        LOGGER.info("Creating parking session via simulation flow: {}", request);

        return validateRequest(request)
                .then(publishVehicleEntryEvent(request))
                .thenReturn(ResponseEntity.ok().<Void>build())
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().<Void>build()))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).<Void>build()))
                .onErrorResume(Exception.class,
                        e -> {
                            LOGGER.error("Failed to publish vehicle entry event", e);
                            return Mono.just(ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).<Void>build());
                        });
    }

    private Mono<Void> validateRequest(ParkingSessionRequestDto request) {
        if (request.vehicleId() == null) {
            return Mono.error(new IllegalArgumentException("vehicleId is required"));
        }
        if (request.userId() == null || request.userId().isBlank()) {
            return Mono.error(new IllegalArgumentException("userId is required"));
        }
        if (request.locationId() == null || request.locationId().isBlank()) {
            return Mono.error(new IllegalArgumentException("locationId is required"));
        }
        if (request.slotId() == null) {
            return Mono.error(new IllegalArgumentException("slotId is required"));
        }
        if (request.fromDate() == null || request.toDate() == null) {
            return Mono.error(new IllegalArgumentException("fromDate and toDate are required"));
        }
        if (!request.fromDate().isBefore(request.toDate())) {
            return Mono.error(new IllegalArgumentException("fromDate must be before toDate"));
        }

        return vehicleService.getVehicleById(request.vehicleId())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Vehicle not found: " + request.vehicleId())))
                .then(
                        userRepository.findById(request.userId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("User not found: " + request.userId())))
                )
                .then(
                        locationRepository.findById(request.locationId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("Location not found: " + request.locationId())))
                )
                .then(
                        parkingSlotRepository.findById(request.slotId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("Slot not found: " + request.slotId())))
                                .filter(slot -> request.locationId().equals(slot.locationId()))
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("Slot does not belong to location")))
                )
                .then(
                        parkingSlotService.isSlotAvailable(request.slotId())
                                .filter(available -> available)
                                .switchIfEmpty(Mono.error(new IllegalStateException("Slot is not available: " + request.slotId())))
                )
                .then();
    }

    private Mono<Void> publishVehicleEntryEvent(ParkingSessionRequestDto request) {
        return vehicleService.getVehicleById(request.vehicleId())
                .flatMap(vehicle -> {
                    String licensePlate = vehicle.vehicleNumber();
                    String cameraId = "CAM-" + request.locationId() + "-ENTRY-1";

                    CameraEvent event = CameraEvent.of(
                            CameraEventType.CAR_ENTERED,
                            licensePlate,
                            request.vehicleId(),
                            request.userId(),
                            request.locationId(),
                            cameraId
                    );

                    LOGGER.info("Publishing vehicle entry event to camera.event.queue: {}", event);

                    return cameraEventPublisher.recordAndPublish(event)
                            .doOnSuccess(published -> LOGGER.info(
                                    "Vehicle entry event published successfully: eventId={}, plate={}, lot={}",
                                    published.eventId(), published.licensePlate(), published.parkingLotId()))
                            .doOnError(error -> LOGGER.error(
                                    "Failed to publish vehicle entry event for vehicle {}", request.vehicleId(), error))
                            .then();
                });
    }

    @GetMapping
    public Flux<ParkingSessionResponseDto> getAll() {
        return service.getAllSessionsWithDetails();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ParkingSessionDto>> getById(@PathVariable Integer id) {
        return service.getSessionById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(
            @PathVariable Integer id,
            @RequestParam String status,
            @RequestParam Integer currentVersion) {

        return service.updateSessionStatus(id, status, currentVersion)
                .map(rows -> ResponseEntity.ok("Parking session status updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }
}

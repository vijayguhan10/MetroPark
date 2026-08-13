package com.example.Metropark.camera.service;

import java.time.LocalDateTime;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.camera.dto.CameraDto;
import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.event.CameraEventPublisher;
import com.example.Metropark.camera.event.CameraEventType;
import com.example.Metropark.camera.repo.CameraRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class CameraService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CameraService.class);
    private static final Set<String> ALLOWED_STATUSES = Set.of("ACTIVE", "INACTIVE", "MAINTENANCE");
    private static final Set<String> ALLOWED_TYPES = Set.of("ENTRY", "EXIT", "OVERVIEW");

    private final CameraRepository repository;
    private final CameraEventPublisher cameraEventPublisher;

    public CameraService(CameraRepository repository, CameraEventPublisher cameraEventPublisher) {
        this.repository = repository;
        this.cameraEventPublisher = cameraEventPublisher;
    }

    @Transactional
    public Mono<Integer> createCamera(CameraDto dto) {
        LOGGER.info("Creating camera: {}", dto);
        if (dto.cameraName() == null || dto.cameraName().isBlank()) {
            return Mono.error(new IllegalArgumentException("Camera name is required."));
        }
        if (dto.locationId() == null || dto.locationId().isBlank()) {
            return Mono.error(new IllegalArgumentException("Location ID is required."));
        }
        if (dto.cameraType() == null || dto.cameraType().isBlank()) {
            return Mono.error(new IllegalArgumentException("Camera type is required."));
        }

        String status = (dto.status() == null || dto.status().isBlank()) ? "ACTIVE" : dto.status().trim().toUpperCase();
        if (!ALLOWED_STATUSES.contains(status)) {
            return Mono.error(new IllegalArgumentException("Status must be one of: ACTIVE, INACTIVE, MAINTENANCE."));
        }

        String cameraType = dto.cameraType().trim().toUpperCase();
        if (!ALLOWED_TYPES.contains(cameraType)) {
            return Mono.error(new IllegalArgumentException("Camera type must be one of: ENTRY, EXIT, OVERVIEW."));
        }

        LocalDateTime now = LocalDateTime.now();

        CameraDto cleanDto = new CameraDto(
                null,
                dto.cameraName().trim(),
                dto.locationId().trim(),
                cameraType,
                status,
                now,
                now);

        return repository.create(cleanDto)
                .doOnSuccess(cameraId -> LOGGER.info("Camera created successfully, camera id: {}", cameraId))
                .doOnError(e -> LOGGER.error("Error creating camera: {}", e.getMessage()));
    }

    public Flux<CameraDto> getAllCameras() {
        LOGGER.debug("Fetching all cameras");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all cameras successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all cameras: {}", e.getMessage()));
    }

    public Mono<CameraDto> getCameraById(Integer id) {
        LOGGER.debug("Fetching camera by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched camera: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching camera by id {}: {}", id, e.getMessage()));
    }

    public Flux<CameraDto> getCamerasByLocationId(String locationId) {
        LOGGER.debug("Fetching cameras by location id: {}", locationId);
        return repository.findByLocationId(locationId)
                .doOnComplete(() -> LOGGER.debug("Fetched cameras by location id successfully"))
                .doOnError(
                        e -> LOGGER.error("Error fetching cameras by location id {}: {}", locationId, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateCamera(Integer id, CameraDto dto) {
        LOGGER.info("Updating camera id: {} with data: {}", id, dto);
        if (dto.cameraName() == null || dto.cameraName().isBlank()) {
            return Mono.error(new IllegalArgumentException("Camera name is required."));
        }
        if (dto.locationId() == null || dto.locationId().isBlank()) {
            return Mono.error(new IllegalArgumentException("Location ID is required."));
        }
        if (dto.cameraType() == null || dto.cameraType().isBlank()) {
            return Mono.error(new IllegalArgumentException("Camera type is required."));
        }

        String status = (dto.status() == null || dto.status().isBlank()) ? "ACTIVE" : dto.status().trim().toUpperCase();
        if (!ALLOWED_STATUSES.contains(status)) {
            return Mono.error(new IllegalArgumentException("Status must be one of: ACTIVE, INACTIVE, MAINTENANCE."));
        }

        String cameraType = dto.cameraType().trim().toUpperCase();
        if (!ALLOWED_TYPES.contains(cameraType)) {
            return Mono.error(new IllegalArgumentException("Camera type must be one of: ENTRY, EXIT, OVERVIEW."));
        }

        LocalDateTime now = LocalDateTime.now();

        CameraDto cleanDto = new CameraDto(
                id,
                dto.cameraName().trim(),
                dto.locationId().trim(),
                cameraType,
                status,
                dto.createdAt(),
                now);

        return repository.update(id, cleanDto)
                .flatMap(rowsUpdated -> rowsUpdated == 0
                        ? Mono.error(new IllegalStateException("Camera not found."))
                        : Mono.just(rowsUpdated))
                .doOnSuccess(rows -> LOGGER.info("Camera updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating camera id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> deleteCamera(Integer id) {
        LOGGER.info("Deleting camera id: {}", id);
        return repository.delete(id)
                .doOnSuccess(rows -> LOGGER.info("Camera deleted successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error deleting camera id {}: {}", id, e.getMessage()));
    }

    public Mono<Void> captureEntryEvent(String licensePlate, String locationId, String cameraId) {
        return capture(CameraEventType.CAR_ENTERED, licensePlate, locationId, cameraId);
    }

    public Mono<Void> captureExitEvent(String licensePlate, String locationId, String cameraId) {
        return capture(CameraEventType.CAR_EXITED, licensePlate, locationId, cameraId);
    }

    private Mono<Void> capture(CameraEventType type, String licensePlate, String locationId, String cameraId) {
        return cameraEventPublisher
                .recordAndPublish(CameraEvent.of(type, licensePlate, null, null, locationId, cameraId))
                .doOnSuccess(event -> LOGGER.info("Camera {} captured for plate {} at {}",
                        type, licensePlate, locationId))
                .doOnError(e -> LOGGER.error("Error capturing camera {} event for plate {}",
                        type, licensePlate, e))
                .then();
    }
}

package com.example.Metropark.parking.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.event.EventPublisher;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.List;

@Service
public class ParkingSlotService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSlotService.class);

    private final ParkingSlotRepository repository;
    private final RedisStateService redisStateService;
    private final EventPublisher eventPublisher;

    public ParkingSlotService(ParkingSlotRepository repository, RedisStateService redisStateService,
            EventPublisher eventPublisher) {
        this.repository = repository;
        this.redisStateService = redisStateService;
        this.eventPublisher = eventPublisher;
    }

    @Transactional
    public Mono<Integer> createSlot(ParkingSlotDto dto) {
        LOGGER.info("Creating parking slot: {}", dto);
        if (dto.locationId() == null || dto.displayCode() == null || dto.sensorId() == null) {
            return Mono.error(new IllegalArgumentException("Location ID, Display Code, and Sensor ID are required."));
        }

        String status = (dto.currentStatus() == null || dto.currentStatus().isBlank())
                ? "AVAILABLE"
                : dto.currentStatus().trim().toUpperCase();

        ParkingSlotDto cleanDto = new ParkingSlotDto(
                dto.slotId(),
                dto.locationId(),
                dto.displayCode().trim().toUpperCase(),
                dto.vehicleTypeId(),
                dto.reservationClassId(),
                dto.sensorId().trim(),
                status);

        return repository.create(cleanDto)
                .flatMap(slotId -> {
                    ParkingSlotDto savedDto = new ParkingSlotDto(
                            slotId,
                            cleanDto.locationId(),
                            cleanDto.displayCode(),
                            cleanDto.vehicleTypeId(),
                            cleanDto.reservationClassId(),
                            cleanDto.sensorId(),
                            cleanDto.currentStatus());

                    long version = 1;

                    // saveSlotState writes the version key to exactly `version`. The old
                    // code additionally called incrementVersion, leaving Redis at 2 while
                    // the event announced 1, so every consumer-side freshness check
                    // (currentVersion < eventVersion) rejected the event it had just been
                    // sent.
                    return redisStateService.saveSlotState(toSlotEventPayload(savedDto), version)
                            .flatMap(stored -> eventPublisher.publishSlotCreated(stored, version))
                            .thenReturn(slotId);
                })
                .doOnSuccess(slotId -> LOGGER.info("Parking slot created successfully, slot id: {}", slotId))
                .doOnError(e -> LOGGER.error("Error creating parking slot: {}", e.getMessage()));
    }

    @Transactional
    public Mono<Integer> createSlots(List<ParkingSlotDto> dtos) {
        if (dtos == null || dtos.isEmpty()) {
            return Mono.error(new IllegalArgumentException("Parking slots list cannot be empty."));
        }

        LOGGER.info("Creating {} parking slots", dtos.size());

        for (ParkingSlotDto dto : dtos) {
            if (dto.locationId() == null || dto.displayCode() == null || dto.sensorId() == null) {
                return Mono.error(new IllegalArgumentException(
                        "Location ID, Display Code, and Sensor ID are required for all slots."));
            }
        }

        return Flux.fromIterable(dtos)
                .flatMap(this::createSlot)
                .count()
                .map(Long::intValue)
                .doOnSuccess(total -> LOGGER.info("Total parking slots created: {}", total))
                .doOnError(e -> LOGGER.error("Error creating parking slots: {}", e.getMessage()));
    }

    public Flux<ParkingSlotDto> getAllSlots() {
        LOGGER.debug("Fetching all parking slots");
        // Try Redis first, fallback to DB
        return redisStateService.getAllSlots()
                .map(this::toParkingSlotDto)
                .switchIfEmpty(repository.findAll())
                .doOnComplete(() -> LOGGER.debug("Fetched all parking slots successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all parking slots: {}", e.getMessage()));
    }

    public Mono<ParkingSlotDto> getSlotById(Integer id) {
        LOGGER.debug("Fetching parking slot by id: {}", id);
        // Try Redis first, fallback to DB
        return redisStateService.getSlot(id)
                .map(this::toParkingSlotDto)
                .switchIfEmpty(repository.findById(id))
                .doOnSuccess(dto -> LOGGER.debug("Fetched parking slot: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching parking slot by id {}: {}", id, e.getMessage()));
    }

    /**
     * Redis first, then publish. No PostgreSQL write happens here at all - the
     * consumer performs it once the event is delivered. Publishing the payload that
     * Redis returned (rather than a hand-built stub with null locationId,
     * displayCode and sensorId) is what stops the consumer from overwriting the
     * cached slot with nulls.
     */
    public Mono<Integer> updateSlotStatus(Integer id, String status) {
        LOGGER.info("Updating parking slot status id: {} to status: {}", id, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        String normalizedStatus = status.trim().toUpperCase();

        return currentSlotPayload(id)
                .switchIfEmpty(Mono.error(new IllegalStateException("Update failed: Slot not found.")))
                .flatMap(existing -> redisStateService.incrementVersion("slot", id.toString())
                        .flatMap(version -> redisStateService.saveSlotState(
                                new SlotEventPayload(
                                        existing.slotId(),
                                        existing.locationId(),
                                        existing.displayCode(),
                                        existing.vehicleTypeId(),
                                        existing.reservationClassId(),
                                        existing.sensorId(),
                                        normalizedStatus,
                                        LocalDateTime.now()),
                                version)
                                // flatMap on the stored value: the publish cannot run
                                // before the Redis write has completed successfully.
                                .flatMap(stored -> eventPublisher.publishSlotUpdated(stored, version))
                                .thenReturn(1)))
                .doOnSuccess(rows -> LOGGER.info(
                        "Parking slot {} set to {} in Redis and published for persistence", id, normalizedStatus))
                .doOnError(e -> LOGGER.error("Error updating parking slot status id {}: {}", id, e.getMessage()));
    }

    /**
     * Slot as the real-time store sees it, falling back to PostgreSQL the first time
     * a slot is touched after startup so the static columns are never lost.
     */
    private Mono<SlotEventPayload> currentSlotPayload(Integer id) {
        return redisStateService.getSlot(id)
                .switchIfEmpty(repository.findById(id).map(this::toSlotEventPayload));
    }

    private SlotEventPayload toSlotEventPayload(ParkingSlotDto dto) {
        return new SlotEventPayload(
                dto.slotId(),
                dto.locationId(),
                dto.displayCode(),
                dto.vehicleTypeId(),
                dto.reservationClassId(),
                dto.sensorId(),
                dto.currentStatus(),
                LocalDateTime.now());
    }

    // Removed: toSlotEventPayload(slotId, status). "will be fetched from Redis if
    // needed" never happened - the consumer wrote this stub over the cached slot,
    // discarding locationId, displayCode and sensorId. updateSlotStatus now merges
    // onto the slot Redis already holds and publishes that.

    private ParkingSlotDto toParkingSlotDto(SlotEventPayload payload) {
        return new ParkingSlotDto(
                payload.slotId(),
                payload.locationId(),
                payload.displayCode(),
                payload.vehicleTypeId(),
                payload.reservationClassId(),
                payload.sensorId(),
                payload.currentStatus());
    }
}
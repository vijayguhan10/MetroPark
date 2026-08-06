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

    public ParkingSlotService(ParkingSlotRepository repository, RedisStateService redisStateService, EventPublisher eventPublisher) {
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
                .flatMap(rows -> {
                    // Get the generated slot ID (assuming it's returned or we need to fetch it)
                    // For now, we'll use a version of 1 for new slots
                    long version = 1;
                    
                    // Save to Redis
                    return redisStateService.saveSlot(cleanDto, version)
                            .then(redisStateService.incrementVersion("slot", cleanDto.slotId().toString()))
                            .then(eventPublisher.publishSlotCreated(toSlotEventPayload(cleanDto), version))
                            .thenReturn(rows);
                })
                .doOnSuccess(rows -> LOGGER.info("Parking slot created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating parking slot: {}", e.getMessage()));
    }

    @Transactional
    public Mono<Integer> createSlots(List<ParkingSlotDto> dtos) {
        LOGGER.info("Creating {} parking slots", dtos.size());
        if (dtos == null || dtos.isEmpty()) {
            return Mono.error(new IllegalArgumentException("Parking slots list cannot be empty."));
        }

        for (ParkingSlotDto dto : dtos) {
            if (dto.locationId() == null || dto.displayCode() == null || dto.sensorId() == null) {
                return Mono.error(new IllegalArgumentException("Location ID, Display Code, and Sensor ID are required for all slots."));
            }
        }

        return Flux.fromIterable(dtos)
                .flatMap(this::createSlot)
                .reduce(0, Integer::sum)
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

    @Transactional
    public Mono<Integer> updateSlotStatus(Integer id, String status) {
        LOGGER.info("Updating parking slot status id: {} to status: {}", id, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        String normalizedStatus = status.trim().toUpperCase();

        return repository.updateStatus(id, normalizedStatus)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Update failed: Slot not found or status unchanged."));
                    }
                    
                    // Increment version and update Redis
                    return redisStateService.incrementVersion("slot", id.toString())
                            .flatMap(version -> redisStateService.updateSlotStatus(id, normalizedStatus, version)
                                    .then(eventPublisher.publishSlotUpdated(toSlotEventPayload(id, normalizedStatus), version)))
                            .thenReturn(rowsUpdated);
                })
                .doOnSuccess(rows -> LOGGER.info("Parking slot status updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating parking slot status id {}: {}", id, e.getMessage()));
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
                LocalDateTime.now()
        );
    }

    private SlotEventPayload toSlotEventPayload(Integer slotId, String status) {
        return new SlotEventPayload(
                slotId,
                null, // locationId - will be fetched from Redis if needed
                null, // displayCode
                null, // vehicleTypeId
                null, // reservationClassId
                null, // sensorId
                status,
                LocalDateTime.now()
        );
    }

    private ParkingSlotDto toParkingSlotDto(SlotEventPayload payload) {
        return new ParkingSlotDto(
                payload.slotId(),
                payload.locationId(),
                payload.displayCode(),
                payload.vehicleTypeId(),
                payload.reservationClassId(),
                payload.sensorId(),
                payload.currentStatus()
        );
    }
}
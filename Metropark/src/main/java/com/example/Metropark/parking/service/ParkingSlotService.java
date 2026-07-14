package com.example.Metropark.parking.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ParkingSlotService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSlotService.class);

    private final ParkingSlotRepository repository;

    public ParkingSlotService(ParkingSlotRepository repository) {
        this.repository = repository;
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
                .doOnSuccess(rows -> LOGGER.info("Parking slot created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating parking slot: {}", e.getMessage()));
    }

    public Flux<ParkingSlotDto> getAllSlots() {
        LOGGER.debug("Fetching all parking slots");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all parking slots successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all parking slots: {}", e.getMessage()));
    }

    public Mono<ParkingSlotDto> getSlotById(Integer id) {
        LOGGER.debug("Fetching parking slot by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched parking slot: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching parking slot by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateSlotStatus(Integer id, String status) {
        LOGGER.info("Updating parking slot status id: {} to status: {}", id, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return repository.updateStatus(id, status.trim().toUpperCase())
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Update failed: Slot not found or status unchanged."));
                    }
                    LOGGER.info("Parking slot status updated successfully, rows affected: {}", rowsUpdated);
                    return Mono.just(rowsUpdated);
                })
                .doOnError(e -> LOGGER.error("Error updating parking slot status id {}: {}", id, e.getMessage()));
    }
}
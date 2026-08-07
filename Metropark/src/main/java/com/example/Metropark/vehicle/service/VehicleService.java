package com.example.Metropark.vehicle.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.dto.VehicleResponseDto;
import com.example.Metropark.vehicle.repo.VehicleRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class VehicleService {

    private static final Logger LOGGER = LoggerFactory.getLogger(VehicleService.class);

    private final VehicleRepository repository;

    public VehicleService(VehicleRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> registerVehicle(VehicleDto dto) {
        LOGGER.info("Registering vehicle: {}", dto);
        if (dto.userId() == null || dto.vehicleTypeId() == null) {
            return Mono.error(new IllegalArgumentException("User ID and Vehicle Type ID are required."));
        }
        if (dto.vehicleNumber() == null || dto.vehicleNumber().isBlank()) {
            return Mono.error(new IllegalArgumentException("Vehicle registration number is required."));
        }

        String cleanVehicleNumber = dto.vehicleNumber()
                .replaceAll("[\\s\\-]", "")
                .toUpperCase();

        Boolean isActive = (dto.isActive() != null) ? dto.isActive() : Boolean.TRUE;
        LocalDateTime now = LocalDateTime.now();

        VehicleDto cleanDto = new VehicleDto(
                null, // Let DB generate ID
                dto.userId(),
                cleanVehicleNumber,
                dto.vehicleTypeId(),
                dto.brand() != null ? dto.brand().trim() : null,
                dto.model() != null ? dto.model().trim() : null,
                dto.color() != null ? dto.color().trim() : null,
                isActive,
                now,
                now);

        return repository.create(cleanDto)
                .doOnSuccess(rows -> LOGGER.info("Vehicle registered successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error registering vehicle: {}", e.getMessage()));
    }

    public Flux<VehicleResponseDto> getAllVehicles() {
        LOGGER.debug("Fetching all vehicles");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all vehicles successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all vehicles: {}", e.getMessage()));
    }

    public Mono<VehicleDto> getVehicleById(Integer id) {
        LOGGER.debug("Fetching vehicle by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched vehicle: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching vehicle by id {}: {}", id, e.getMessage()));
    }

    public Flux<VehicleResponseDto> getVehiclesByUserId(String userId) {

        LOGGER.info("Fetching vehicles for user: {}", userId);

        return repository.findByUserId(userId)
                .switchIfEmpty(Flux.error(
                        new IllegalArgumentException(
                                "No vehicles found for user: " + userId)))
                .doOnComplete(() -> LOGGER.info("Vehicles fetched successfully for user: {}", userId))
                .doOnError(error -> LOGGER.error("Error fetching vehicles for user {}",
                        userId,
                        error));
    }

    public Mono<VehicleDto> getVehicleByNumber(String vehicleNumber) {
        LOGGER.debug("Fetching vehicle by number: {}", vehicleNumber);
        return repository.findByVehicleNumber(vehicleNumber)
                .doOnSuccess(dto -> LOGGER.debug("Fetched vehicle: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching vehicle by number {}: {}", vehicleNumber, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> toggleVehicleStatus(Integer id, Boolean isActive) {
        LOGGER.info("Toggling vehicle status id: {} to isActive: {}", id, isActive);
        if (isActive == null) {
            return Mono.error(new IllegalArgumentException("Status (isActive) must be true or false."));
        }

        return repository.updateActiveStatus(id, isActive)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Vehicle not found."));
                    }
                    LOGGER.info("Vehicle status toggled successfully, rows affected: {}", rowsUpdated);
                    return Mono.just(rowsUpdated);
                })
                .doOnError(e -> LOGGER.error("Error toggling vehicle status id {}: {}", id, e.getMessage()));
    }
}
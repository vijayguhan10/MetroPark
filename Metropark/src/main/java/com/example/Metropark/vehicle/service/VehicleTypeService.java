package com.example.Metropark.vehicle.service;

import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.repo.VehicleTypeRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class VehicleTypeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(VehicleTypeService.class);

    private final VehicleTypeRepository repository;

    public VehicleTypeService(VehicleTypeRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createVehicleType(VehicleTypeDto dto) {
        LOGGER.info("Creating vehicle type: {}", dto);
        // 1. Validation: Prevent null or completely empty strings
        if (dto.typeDisplayName() == null || dto.typeDisplayName().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Vehicle type display name cannot be empty."));
        }

        // 2. Sanitization: Trim accidental leading/trailing whitespace
        String cleanName = dto.typeDisplayName().trim();

        // 3. Rebuild DTO with clean data
        VehicleTypeDto cleanDto = new VehicleTypeDto(dto.vehicleTypeId(), cleanName);

        return repository.create(cleanDto)
                .doOnSuccess(rows -> LOGGER.info("Vehicle type created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating vehicle type: {}", e.getMessage()));
    }

    public Flux<VehicleTypeDto> getAllVehicleTypes() {
        LOGGER.debug("Fetching all vehicle types");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all vehicle types successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all vehicle types: {}", e.getMessage()));
    }

    public Mono<VehicleTypeDto> getVehicleTypeById(Integer id) {
        LOGGER.debug("Fetching vehicle type by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched vehicle type: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching vehicle type by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateVehicleType(Integer id, VehicleTypeDto dto) {
        LOGGER.info("Updating vehicle type id: {} with data: {}", id, dto);
        if (dto.typeDisplayName() == null || dto.typeDisplayName().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Vehicle type display name cannot be empty."));
        }
        
        String cleanName = dto.typeDisplayName().trim();
        VehicleTypeDto cleanDto = new VehicleTypeDto(dto.vehicleTypeId(), cleanName);
        
        return repository.update(id, cleanDto)
                .doOnSuccess(rows -> LOGGER.info("Vehicle type updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating vehicle type id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> deleteVehicleType(Integer id) {
        LOGGER.info("Deleting vehicle type id: {}", id);
        return repository.delete(id)
                .doOnSuccess(rows -> LOGGER.info("Vehicle type deleted successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error deleting vehicle type id {}: {}", id, e.getMessage()));
    }
}
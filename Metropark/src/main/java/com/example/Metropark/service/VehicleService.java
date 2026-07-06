package com.example.Metropark.service;

import com.example.Metropark.dto.VehicleDto;
import com.example.Metropark.repo.VehicleRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class VehicleService {

    private final VehicleRepository repository;

    public VehicleService(VehicleRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> registerVehicle(VehicleDto dto) {
        if (dto.userId() == null || dto.vehicleTypeId() == null) {
            return Mono.error(new IllegalArgumentException("User ID and Vehicle Type ID are required."));
        }
        if (dto.vehicleNumber() == null || dto.vehicleNumber().isBlank()) {
            return Mono.error(new IllegalArgumentException("Vehicle registration number is required."));
        }

        // Robust Data Sanitization: Remove ALL spaces, hyphens, and make uppercase
        String cleanVehicleNumber = dto.vehicleNumber()
                .replaceAll("[\\s\\-]", "")
                .toUpperCase();

        Boolean isActive = (dto.isActive() != null) ? dto.isActive() : true;
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
                now
        );

        return repository.create(cleanDto);
    }

    public Flux<VehicleDto> getAllVehicles() {
        return repository.findAll();
    }

    public Mono<VehicleDto> getVehicleById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> toggleVehicleStatus(Integer id, Boolean isActive) {
        if (isActive == null) {
            return Mono.error(new IllegalArgumentException("Status (isActive) must be true or false."));
        }
        
        return repository.updateActiveStatus(id, isActive)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Vehicle not found."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
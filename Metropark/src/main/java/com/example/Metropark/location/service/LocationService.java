package com.example.Metropark.location.service;

import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class LocationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationService.class);

    private final LocationRepository repository;

    public LocationService(LocationRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createLocation(LocationDto dto) {
        LOGGER.info("Creating location: {}", dto);
        // Automatically generate an ID like "LOC-304"
        String generatedId = "LOC-" + String.format("%03d", new Random().nextInt(1000));
        
        // Default to ACTIVE if the user didn't provide a status
        String finalStatus = (dto.status() != null) ? dto.status() : "ACTIVE";

        // Rebuild the DTO with the generated data
        LocationDto locationToSave = new LocationDto(
                generatedId, 
                dto.typeId(), 
                dto.locationName(), 
                dto.city(), 
                finalStatus
        );

        return repository.create(locationToSave)
                .doOnSuccess(rows -> LOGGER.info("Location created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating location: {}", e.getMessage()));
    }

    public Flux<LocationDto> getAllLocations() {
        LOGGER.debug("Fetching all locations");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all locations successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all locations: {}", e.getMessage()));
    }

    public Mono<LocationDto> getLocationById(String id) {
        LOGGER.debug("Fetching location by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched location: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching location by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateLocationStatus(String id, String status) {
        LOGGER.info("Updating location status id: {} to status: {}", id, status);
        return repository.updateStatus(id, status)
                .doOnSuccess(rows -> LOGGER.info("Location status updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating location status id {}: {}", id, e.getMessage()));
    }
}
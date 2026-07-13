package com.example.Metropark.location.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.location.dto.LocationTypeDto;
import com.example.Metropark.location.repo.LocationTypeRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class LocationTypeService {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationTypeService.class);

    private final LocationTypeRepository repository;

    public LocationTypeService(LocationTypeRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createLocationType(LocationTypeDto dto) {
        LOGGER.info("Creating location type: {}", dto);
        return repository.create(dto)
                .doOnSuccess(rows -> LOGGER.info("Location type created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating location type: {}", e.getMessage()));
    }

    public Flux<LocationTypeDto> getAllLocationTypes() {
        LOGGER.debug("Fetching all location types");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all location types successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all location types: {}", e.getMessage()));
    }

    public Mono<LocationTypeDto> getLocationTypeById(Integer id) {
        LOGGER.debug("Fetching location type by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched location type: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching location type by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateLocationType(Integer id, LocationTypeDto dto) {
        LOGGER.info("Updating location type id: {} with data: {}", id, dto);
        return repository.update(id, dto)
                .doOnSuccess(rows -> LOGGER.info("Location type updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating location type id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> deleteLocationType(Integer id) {
        LOGGER.info("Deleting location type id: {}", id);
        return repository.delete(id)
                .doOnSuccess(rows -> LOGGER.info("Location type deleted successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error deleting location type id {}: {}", id, e.getMessage()));
    }
}

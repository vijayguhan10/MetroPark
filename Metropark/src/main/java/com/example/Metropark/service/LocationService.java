package com.example.Metropark.service;

import com.example.Metropark.dto.LocationDto;
import com.example.Metropark.repo.LocationRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Random;

@Service
public class LocationService {

    private final LocationRepository repository;

    public LocationService(LocationRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createLocation(LocationDto dto) {
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

        return repository.create(locationToSave);
    }

    public Flux<LocationDto> getAllLocations() {
        return repository.findAll();
    }

    public Mono<LocationDto> getLocationById(String id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateLocationStatus(String id, String status) {
        return repository.updateStatus(id, status);
    }
}
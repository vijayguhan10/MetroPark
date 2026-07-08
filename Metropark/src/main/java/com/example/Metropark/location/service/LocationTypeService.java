package com.example.Metropark.location.service;

import org.springframework.stereotype.Service;

import com.example.Metropark.location.dto.LocationTypeDto;
import com.example.Metropark.location.repo.LocationTypeRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class LocationTypeService {

    private final LocationTypeRepository repository;

    public LocationTypeService(LocationTypeRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createLocationType(LocationTypeDto dto) {
        return repository.create(dto);
    }

    public Flux<LocationTypeDto> getAllLocationTypes() {
        return repository.findAll();
    }

    public Mono<LocationTypeDto> getLocationTypeById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateLocationType(Integer id, LocationTypeDto dto) {
        return repository.update(id, dto);
    }

    public Mono<Integer> deleteLocationType(Integer id) {
        return repository.delete(id);
    }
}
package com.example.Metropark.location.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.service.LocationService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/locations")
public class LocationController {

    private static final Logger LOGGER = LoggerFactory.getLogger(LocationController.class);

    private final LocationService service;

    public LocationController(LocationService service) {
        this.service = service;
    }

    @PostMapping
    @Transactional

    public Mono<ResponseEntity<String>> create(@RequestBody LocationDto dto) {
        LOGGER.info("Creating location: {}", dto);
        return service.createLocation(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Location created successfully."));
    }

    @GetMapping
    public Flux<LocationDto> getAll() {
        return service.getAllLocations();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<LocationDto>> getById(@PathVariable String id) {
        return service.getLocationById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(@PathVariable String id, @RequestParam String status) {
        return service.updateLocationStatus(id, status.toUpperCase())
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated > 0) {
                        return Mono.just(ResponseEntity.ok("Location status updated successfully."));
                    } else {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                });
    }
}
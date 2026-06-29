package com.example.Metropark.controller;

import com.example.Metropark.dto.LocationDto;
import com.example.Metropark.service.LocationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/locations")
public class LocationController {

    private final LocationService service;

    public LocationController(LocationService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody LocationDto dto) {
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
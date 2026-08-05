package com.example.Metropark.vehicle.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.service.VehicleTypeService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/vehicle-types")
public class VehicleTypeController {

    private final VehicleTypeService service;

    public VehicleTypeController(VehicleTypeService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody VehicleTypeDto dto) {
        return service.createVehicleType(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Vehicle type created successfully."))

                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<VehicleTypeDto> getAll() {
        return service.getAllVehicleTypes();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<VehicleTypeDto>> getById(@PathVariable Integer id) {
        return service.getVehicleTypeById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(
            @PathVariable Integer id,
            @RequestBody VehicleTypeDto dto) {

        return service.updateVehicleType(id, dto)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated > 0) {
                        return Mono.just(
                                ResponseEntity.ok("Vehicle type updated successfully"));
                    } else {
                        return Mono.just(
                                ResponseEntity.status(HttpStatus.NOT_FOUND)
                                        .body("Vehicle type not found"));
                    }
                })
                .onErrorResume(ex -> Mono.just(
                        ResponseEntity.internalServerError()
                                .body(ex.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Integer id) {
        return service.deleteVehicleType(id)
                .flatMap(rowsDeleted -> {
                    if (rowsDeleted > 0) {
                        return Mono.just(ResponseEntity.ok("Vehicle type deleted successfully."));
                    } else {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                });
    }
}
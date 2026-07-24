package com.example.Metropark.vehicle.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.dto.VehicleResponseDto;
import com.example.Metropark.vehicle.service.VehicleService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/vehicles")
public class VehicleController {

    private final VehicleService service;

    public VehicleController(VehicleService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> register(@RequestBody VehicleDto dto) {
        return service.registerVehicle(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Vehicle registered successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<VehicleResponseDto> getAll() {
        return service.getAllVehicles();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<VehicleDto>> getById(@PathVariable Integer id) {
        return service.getVehicleById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(
            @PathVariable Integer id, 
            @RequestParam Boolean isActive) {
            
        return service.toggleVehicleStatus(id, isActive)
                .map(rows -> ResponseEntity.ok("Vehicle status updated successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class, e -> 
                        Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage())));
    }
}
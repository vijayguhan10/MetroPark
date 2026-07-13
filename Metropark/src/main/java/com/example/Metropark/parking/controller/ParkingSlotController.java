package com.example.Metropark.parking.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.service.ParkingSlotService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/parking-slots")
public class ParkingSlotController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSlotController.class);

    private final ParkingSlotService service;

    public ParkingSlotController(ParkingSlotService service) {
        this.service = service;
    }

    @PostMapping
    @Transactional

    public Mono<ResponseEntity<String>> create(@RequestBody ParkingSlotDto dto) {
        LOGGER.info("Creating parking slot: {}", dto);
        return service.createSlot(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Parking slot registered successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<ParkingSlotDto> getAll() {
        return service.getAllSlots();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ParkingSlotDto>> getById(@PathVariable Integer id) {
        return service.getSlotById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // Patches the status using the Optimistic Lock logic
    // @PatchMapping("/{id}/status")
    // public Mono<ResponseEntity<String>> updateStatus(
    // @PathVariable Integer id,
    // @RequestParam String status,
    // @RequestParam Integer currentVersion) {

    // return service.updateSlotStatus(id, status, currentVersion)
    // .map(rows -> ResponseEntity.ok("Slot occupancy status updated
    // successfully."))
    // .onErrorResume(IllegalArgumentException.class, e ->
    // Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
    // .onErrorResume(IllegalStateException.class, e ->
    // Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    // }
}
package com.example.Metropark.parking.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.service.ParkingSessionService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/parking-sessions")
public class ParkingSessionController {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSessionController.class);

    private final ParkingSessionService service;

    public ParkingSessionController(ParkingSessionService service) {
        this.service = service;
    }

    @PostMapping
     
    public Mono<ResponseEntity<String>> create(@RequestBody ParkingSessionDto dto) {
        LOGGER.info("Creating parking session: {}", dto);
        return service.createSession(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Parking session initiated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<ParkingSessionDto> getAll() {
        return service.getAllSessions();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ParkingSessionDto>> getById(@PathVariable Integer id) {
        return service.getSessionById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(
            @PathVariable Integer id,
            @RequestParam String status,
            @RequestParam Integer currentVersion) {

        return service.updateSessionStatus(id, status, currentVersion)
                .map(rows -> ResponseEntity.ok("Parking session status updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }
}
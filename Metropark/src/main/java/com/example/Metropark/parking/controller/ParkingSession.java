package com.example.Metropark.parking.controller;

import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.service.ParkingSessionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/parking-sessions")
public class ParkingSessionController {

    private static final Logger LOGGER
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody ParkingSessionDto dto) {
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
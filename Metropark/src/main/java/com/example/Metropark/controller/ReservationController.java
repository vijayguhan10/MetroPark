package com.example.Metropark.controller;

import com.example.Metropark.dto.ReservationDto;
import com.example.Metropark.service.ReservationService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/reservations")
public class ReservationController {

    private final ReservationService service;

    public ReservationController(ReservationService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody ReservationDto dto) {
        return service.createReservation(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Reservation placed successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<ReservationDto> getAll() {
        return service.getAllReservations();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ReservationDto>> getById(@PathVariable Integer id) {
        return service.getReservationById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(
            @PathVariable Integer id, 
            @RequestParam String status, 
            @RequestParam Integer currentVersion) {
            
        return service.updateStatus(id, status, currentVersion)
                .map(rows -> ResponseEntity.ok("Reservation status updated."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class, e -> 
                        Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }
}
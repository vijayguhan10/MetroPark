package com.example.Metropark.reservation.controller;

import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.service.ReservationClassService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/reservation-classes")
public class ReservationClassController {

    private final ReservationClassService service;

    public ReservationClassController(ReservationClassService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody ReservationClassDto dto) {
        return service.createReservationClass(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Reservation class created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<ReservationClassDto> getAll() {
        return service.getAllReservationClasses();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ReservationClassDto>> getById(@PathVariable Integer id) {
        return service.getReservationClassById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Integer id, @RequestBody ReservationClassDto dto) {
        return service.updateReservationClass(id, dto)
                .flatMap(rows -> {
                    if (rows > 0) {
                        return Mono.just(ResponseEntity.ok("Reservation class updated successfully."));
                    } else {
                        return Mono.just(
                                ResponseEntity.status(HttpStatus.NOT_FOUND)
                                        .body("Reservation class not found"));
                    }
                })
                .onErrorResume(ex -> Mono.just(
                        ResponseEntity.internalServerError()
                                .body(ex.getMessage())));

    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Integer id) {
        return service.deleteReservationClass(id)
                .flatMap(rows -> rows > 0
                        ? Mono.just(ResponseEntity.ok("Reservation class deleted successfully."))
                        : Mono.just(ResponseEntity.notFound().build()));
    }
}
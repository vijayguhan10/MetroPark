package com.example.Metropark.payments.controller;

import com.example.Metropark.payments.dto.ReservationBillingTypeDto;
import com.example.Metropark.payments.service.ReservationBillingTypeService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/reservation-billing-types")
public class ReservationBillingTypeController {

    private final ReservationBillingTypeService service;

    public ReservationBillingTypeController(ReservationBillingTypeService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@Valid @RequestBody ReservationBillingTypeDto dto) {
        return service.createBillingType(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Billing type created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @GetMapping
    public Flux<ReservationBillingTypeDto> getAll() {
        return service.getAllBillingTypes();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ReservationBillingTypeDto>> getById(@PathVariable Long id) {
        return service.getBillingTypeById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Long id,
            @Valid @RequestBody ReservationBillingTypeDto dto) {
        return service.updateBillingType(id, dto)
                .map(rows -> ResponseEntity.ok("Billing type updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Long id) {
        return service.deleteBillingType(id)
                .map(rows -> rows > 0
                        ? ResponseEntity.ok("Billing type deleted successfully.")
                        : ResponseEntity.notFound().build());
    }
}
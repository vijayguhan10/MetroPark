package com.example.Metropark.payments.controller;

import com.example.Metropark.payments.dto.PricingRateDto;
import com.example.Metropark.payments.service.PricingRateService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/pricing-rates")
public class PricingRateController {

    private final PricingRateService service;

    public PricingRateController(PricingRateService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@Valid @RequestBody PricingRateDto dto) {
        return service.createPricingRate(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Pricing rate created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @GetMapping
    public Flux<PricingRateDto> getAll() {
        return service.getAllPricingRates();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<PricingRateDto>> getById(@PathVariable Long id) {
        return service.getPricingRateById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/resolve")
    public Mono<ResponseEntity<PricingRateDto>> resolve(
            @RequestParam String locationId,
            @RequestParam Integer vehicleTypeId,
            @RequestParam Integer reservationClassId,
            @RequestParam Integer billingTypeId,
            @RequestParam(required = false) LocalDateTime effectiveAt) {

        return service.resolveRate(locationId, vehicleTypeId, reservationClassId, billingTypeId, effectiveAt)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Long id, @Valid @RequestBody PricingRateDto dto) {
        return service.updatePricingRate(id, dto)
                .map(rows -> ResponseEntity.ok("Pricing rate updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Long id) {
        return service.deletePricingRate(id)
                .map(rows -> rows > 0
                        ? ResponseEntity.ok("Pricing rate deleted successfully.")
                        : ResponseEntity.notFound().build());
    }
}
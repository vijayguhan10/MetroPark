package com.example.Metropark.payments.controller;

import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.service.PaymentMethodService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/payment-methods")
public class PaymentMethodController {

    private final PaymentMethodService service;

    public PaymentMethodController(PaymentMethodService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@Valid @RequestBody PaymentMethodDto dto) {
        return service.createPaymentMethod(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Payment method created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @GetMapping
    public Flux<PaymentMethodDto> getAll() {
        return service.getAllPaymentMethods();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<PaymentMethodDto>> getById(@PathVariable Long id) {
        return service.getPaymentMethodById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Long id, @Valid @RequestBody PaymentMethodDto dto) {
        return service.updatePaymentMethod(id, dto)
                .map(rows -> ResponseEntity.ok("Payment method updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Long id) {
        return service.deletePaymentMethod(id)
                .map(rows -> rows > 0
                        ? ResponseEntity.ok("Payment method deleted successfully.")
                        : ResponseEntity.notFound().build());
    }
}
package com.example.Metropark.payments.controller;

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
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.service.PaymentService;

import jakarta.validation.Valid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    @PostMapping

    public Mono<ResponseEntity<String>> create(@Valid @RequestBody PaymentDto dto) {
        LOGGER.info("Creating payment: {}", dto);
        return service.createPayment(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED)
                        .body("Payment created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(e.getMessage())));
    }

    @GetMapping
    public Flux<PaymentDto> getAll() {
        return service.getAllPayments();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<PaymentDto>> getById(@PathVariable Long id) {
        return service.getPaymentById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/reference/{transactionReference}")
    public Mono<ResponseEntity<PaymentDto>> getByReference(@PathVariable String transactionReference) {
        return service.getPaymentByTransactionReference(transactionReference)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/session/{sessionId}")
    public Flux<PaymentDto> getBySessionId(@PathVariable Integer sessionId) {
        return service.getPaymentsBySessionId(sessionId);
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(@PathVariable Long id,
            @Valid @RequestBody PaymentStatusUpdateDto dto) {
        return service.updatePaymentStatus(id, dto)
                .map(rows -> ResponseEntity.ok("Payment status updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(e.getMessage())));
    }
}

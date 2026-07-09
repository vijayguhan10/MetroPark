package com.example.Metropark.payments.controller;

import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentGatewayUpdateDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.service.PaymentService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/payments")
public class PaymentController {

    private final PaymentService service;

    public PaymentController(PaymentService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@Valid @RequestBody PaymentDto dto) {
        return service.createPayment(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Payment created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
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

    @GetMapping("/reservation/{reservationId}")
    public Flux<PaymentDto> getByReservationId(@PathVariable Integer reservationId) {
        return service.getPaymentsByReservationId(reservationId);
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
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }

    @PatchMapping("/{id}/gateway-response")
    public Mono<ResponseEntity<String>> updateGatewayResponse(@PathVariable Long id,
            @Valid @RequestBody PaymentGatewayUpdateDto dto) {
        return service.updateGatewayResponse(id, dto)
                .map(rows -> ResponseEntity.ok("Payment gateway response updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    }
}
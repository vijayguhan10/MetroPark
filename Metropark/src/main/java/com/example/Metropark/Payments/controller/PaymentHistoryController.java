package com.example.Metropark.payments.controller;

import com.example.Metropark.payments.dto.PaymentHistoryDto;
import com.example.Metropark.payments.service.PaymentHistoryService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/payment-history")
public class PaymentHistoryController {

    private final PaymentHistoryService service;

    public PaymentHistoryController(PaymentHistoryService service) {
        this.service = service;
    }

    @GetMapping
    public Flux<PaymentHistoryDto> getAll() {
        return service.getAllHistory();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<PaymentHistoryDto>> getById(@PathVariable Long id) {
        return service.getHistoryById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/payment/{paymentId}")
    public Flux<PaymentHistoryDto> getByPaymentId(@PathVariable Long paymentId) {
        return service.getHistoryByPaymentId(paymentId);
    }
}
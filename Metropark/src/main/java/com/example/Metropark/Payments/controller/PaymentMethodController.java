package com.example.Metropark.payments.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.service.PaymentMethodService;

import jakarta.validation.Valid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/payment-methods")
public class PaymentMethodController {

        private static final Logger LOGGER = LoggerFactory.getLogger(PaymentMethodController.class);

        private final PaymentMethodService service;

        public PaymentMethodController(PaymentMethodService service) {
                this.service = service;
        }

        @PostMapping

        public Mono<ResponseEntity<String>> create(@Valid @RequestBody PaymentMethodDto dto) {
                LOGGER.info("Creating payment method: {}", dto);
                return service.createPaymentMethod(dto)
                                .map(rows -> ResponseEntity.status(HttpStatus.CREATED)
                                                .body("Payment method created successfully."))
                                .onErrorResume(IllegalArgumentException.class,
                                                e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                                .onErrorResume(IllegalStateException.class,
                                                e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                                                .body(e.getMessage())));
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
                LOGGER.info("Updating payment method {}: {}", id, dto);
                return service.updatePaymentMethod(id, dto)
                                .map(rows -> ResponseEntity.ok("Payment method updated successfully."))
                                .onErrorResume(IllegalArgumentException.class,
                                                e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                                .onErrorResume(IllegalStateException.class,
                                                e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                                                .body(e.getMessage())));
        }

        @DeleteMapping("/{id}")

        public Mono<ResponseEntity<String>> delete(@PathVariable Long id) {
                LOGGER.info("Deleting payment method: {}", id);
                return service.deletePaymentMethod(id)
                                .map(rows -> rows > 0
                                                ? ResponseEntity.ok("Payment method deleted successfully.")
                                                : ResponseEntity.notFound().build());
        }
}

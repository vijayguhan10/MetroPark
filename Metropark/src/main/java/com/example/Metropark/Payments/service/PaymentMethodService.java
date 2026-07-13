package com.example.Metropark.payments.service;

import java.time.LocalDateTime;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.repo.PaymentMethodRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class PaymentMethodService {

    private static final Set<String> ALLOWED_METHODS = Set.of(
            "CREDIT_CARD", "DEBIT_CARD", "UPI", "DIGITAL_WALLET", "NET_BANKING", "CASH");

    private final PaymentMethodRepository repository;

    public PaymentMethodService(PaymentMethodRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createPaymentMethod(PaymentMethodDto dto) {
        PaymentMethodDto cleanDto = normalize(dto, null);
        return repository.existsByMethodName(cleanDto.methodName())
                .flatMap(exists -> exists
                        ? Mono.error(new IllegalStateException("Payment method already exists."))
                        : repository.create(cleanDto));
    }

    public Flux<PaymentMethodDto> getAllPaymentMethods() {
        return repository.findAll();
    }

    public Mono<PaymentMethodDto> getPaymentMethodById(Long id) {
        return repository.findById(id);
    }

    public Mono<Integer> updatePaymentMethod(Long id, PaymentMethodDto dto) {
        PaymentMethodDto cleanDto = normalize(dto, id);
        return repository.findByMethodName(cleanDto.methodName())
                .flatMap(existing -> existing.methodId() != null && !existing.methodId().equals(id)
                        ? Mono.<Integer>error(new IllegalStateException("Payment method already exists."))
                        : repository.update(id, cleanDto))
                .switchIfEmpty(repository.update(id, cleanDto));
    }

    public Mono<Integer> deletePaymentMethod(Long id) {
        return repository.delete(id);
    }

    public Mono<Void> requireActiveMethod(Integer methodId) {
        return repository.findById(methodId.longValue())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Payment method not found.")))
                .flatMap(method -> {
                    if (!Boolean.TRUE.equals(method.isActive())) {
                        return Mono.error(new IllegalStateException("Payment method is disabled."));
                    }
                    return Mono.empty();
                });
    }

    private PaymentMethodDto normalize(PaymentMethodDto dto, Long id) {
        if (dto.methodName() == null || dto.methodName().isBlank()) {
            throw new IllegalArgumentException("Payment method name is required.");
        }

        String methodName = dto.methodName().trim().toUpperCase();
        if (!ALLOWED_METHODS.contains(methodName)) {
            throw new IllegalArgumentException(
                    "Payment method must be one of: CREDIT_CARD, DEBIT_CARD, UPI, DIGITAL_WALLET, NET_BANKING, CASH.");
        }

        return new PaymentMethodDto(
                id != null ? id : dto.methodId(),
                methodName,
                dto.isActive() == null ? Boolean.TRUE : dto.isActive(),
                dto.createdAt() == null ? LocalDateTime.now() : dto.createdAt());
    }
}
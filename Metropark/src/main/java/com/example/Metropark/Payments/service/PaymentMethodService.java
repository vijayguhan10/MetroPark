package com.example.Metropark.payments.service;

import java.time.LocalDateTime;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.repo.PaymentMethodRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class PaymentMethodService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentMethodService.class);
    private static final Set<String> ALLOWED_METHODS = Set.of(
            "CREDIT_CARD", "DEBIT_CARD", "UPI", "DIGITAL_WALLET", "NET_BANKING", "CASH");

    private final PaymentMethodRepository repository;

    public PaymentMethodService(PaymentMethodRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createPaymentMethod(PaymentMethodDto dto) {
        LOGGER.info("Creating payment method: {}", dto);
        PaymentMethodDto cleanDto = normalize(dto, null);
        return repository.existsByMethodName(cleanDto.methodName())
                .flatMap(exists -> exists
                        ? Mono.error(new IllegalStateException("Payment method already exists."))
                        : repository.create(cleanDto)
                                .doOnSuccess(rows -> LOGGER.info("Payment method created successfully, rows affected: {}", rows))
                                .doOnError(e -> LOGGER.error("Error creating payment method: {}", e.getMessage())));
    }

    public Flux<PaymentMethodDto> getAllPaymentMethods() {
        LOGGER.debug("Fetching all payment methods");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all payment methods successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all payment methods: {}", e.getMessage()));
    }

    public Mono<PaymentMethodDto> getPaymentMethodById(Long id) {
        LOGGER.debug("Fetching payment method by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched payment method: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching payment method by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updatePaymentMethod(Long id, PaymentMethodDto dto) {
        LOGGER.info("Updating payment method id: {} with data: {}", id, dto);
        PaymentMethodDto cleanDto = normalize(dto, id);
        return repository.findByMethodName(cleanDto.methodName())
                .flatMap(existing -> existing.methodId() != null && !existing.methodId().equals(id)
                        ? Mono.<Integer>error(new IllegalStateException("Payment method already exists."))
                        : repository.update(id, cleanDto)
                                .doOnSuccess(rows -> LOGGER.info("Payment method updated successfully, rows affected: {}", rows))
                                .doOnError(e -> LOGGER.error("Error updating payment method id {}: {}", id, e.getMessage())))
                .switchIfEmpty(repository.update(id, cleanDto)
                        .doOnSuccess(rows -> LOGGER.info("Payment method updated successfully, rows affected: {}", rows))
                        .doOnError(e -> LOGGER.error("Error updating payment method id {}: {}", id, e.getMessage())));
    }

    @Transactional
    public Mono<Integer> deletePaymentMethod(Long id) {
        LOGGER.info("Deleting payment method id: {}", id);
        return repository.delete(id)
                .doOnSuccess(rows -> LOGGER.info("Payment method deleted successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error deleting payment method id {}: {}", id, e.getMessage()));
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
            throw new IllegalArgumentException("Payment method must be one of: CREDIT_CARD, DEBIT_CARD, UPI, DIGITAL_WALLET, NET_BANKING, CASH.");
        }

        return new PaymentMethodDto(
                id != null ? id : dto.methodId(),
                methodName,
                dto.isActive() == null ? Boolean.TRUE : dto.isActive(),
                dto.createdAt() == null ? LocalDateTime.now() : dto.createdAt());
    }
}
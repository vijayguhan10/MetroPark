package com.example.Metropark.payments.service;

import java.time.LocalDateTime;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.example.Metropark.payments.dto.ReservationBillingTypeDto;
import com.example.Metropark.payments.repo.ReservationBillingTypeRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReservationBillingTypeService {

    private static final Set<String> ALLOWED_NAMES = Set.of("HOURLY", "DAILY", "WEEKLY_2", "MONTHLY_1");
    private static final Set<String> ALLOWED_UNITS = Set.of("HOUR", "DAY", "WEEK", "MONTH");

    private final ReservationBillingTypeRepository repository;

    public ReservationBillingTypeService(ReservationBillingTypeRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createBillingType(ReservationBillingTypeDto dto) {
        ReservationBillingTypeDto cleanDto = normalize(dto, null);
        return repository.existsByTypeName(cleanDto.typeName())
                .flatMap(exists -> exists
                        ? Mono.error(new IllegalStateException("Billing type already exists."))
                        : repository.create(cleanDto));
    }

    public Flux<ReservationBillingTypeDto> getAllBillingTypes() {
        return repository.findAll();
    }

    public Mono<ReservationBillingTypeDto> getBillingTypeById(Long id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateBillingType(Long id, ReservationBillingTypeDto dto) {
        ReservationBillingTypeDto cleanDto = normalize(dto, null);
        return repository.findByTypeName(cleanDto.typeName())
                .flatMap(existing -> existing.billingTypeId() != null && !existing.billingTypeId().equals(id)
                        ? Mono.<Integer>error(new IllegalStateException("Billing type already exists."))
                        : repository.update(id, cleanDto))
                .switchIfEmpty(repository.update(id, cleanDto));
    }

    public Mono<Integer> deleteBillingType(Long id) {
        return repository.delete(id);
    }

    private ReservationBillingTypeDto normalize(ReservationBillingTypeDto dto, Long id) {
        if (dto.typeName() == null || dto.typeName().isBlank()) {
            throw new IllegalArgumentException("Billing type name is required.");
        }
        if (dto.durationValue() <= 0) {
            throw new IllegalArgumentException("Duration value must be greater than zero.");
        }
        if (dto.durationUnit() == null || dto.durationUnit().isBlank()) {
            throw new IllegalArgumentException("Duration unit is required.");
        }

        String typeName = dto.typeName().trim().toUpperCase();
        String durationUnit = dto.durationUnit().trim().toUpperCase();

        if (!ALLOWED_NAMES.contains(typeName)) {
            throw new IllegalArgumentException("Billing type must be one of: HOURLY, DAILY, WEEKLY_2, MONTHLY_1.");
        }
        if (!ALLOWED_UNITS.contains(durationUnit)) {
            throw new IllegalArgumentException("Duration unit must be HOUR, DAY, WEEK, or MONTH.");
        }

        return new ReservationBillingTypeDto(
                id != null ? id : dto.billingTypeId(),
                typeName,
                dto.durationValue(),
                durationUnit,
                dto.isSubscription() == null ? Boolean.FALSE : dto.isSubscription(),
                dto.createdAt() == null ? LocalDateTime.now() : dto.createdAt(),
                LocalDateTime.now());
    }
}
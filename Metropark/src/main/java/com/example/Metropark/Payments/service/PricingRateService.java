package com.example.Metropark.payments.service;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.springframework.stereotype.Service;

import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.payments.dto.PricingRateDto;
import com.example.Metropark.payments.repo.PricingRateRepository;
import com.example.Metropark.reservation.repo.ReservationClassRepository;
import com.example.Metropark.vehicle.repo.VehicleTypeRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class PricingRateService {

    private final PricingRateRepository repository;
    private final LocationRepository locationRepository;
    private final VehicleTypeRepository vehicleTypeRepository;
    private final ReservationClassRepository reservationClassRepository;
    private final ReservationBillingTypeService billingTypeService;

    public PricingRateService(
            PricingRateRepository repository,
            LocationRepository locationRepository,
            VehicleTypeRepository vehicleTypeRepository,
            ReservationClassRepository reservationClassRepository,
            ReservationBillingTypeService billingTypeService) {

        this.repository = repository;
        this.locationRepository = locationRepository;
        this.vehicleTypeRepository = vehicleTypeRepository;
        this.reservationClassRepository = reservationClassRepository;
        this.billingTypeService = billingTypeService;
    }

    public Mono<Integer> createPricingRate(PricingRateDto dto) {
        PricingRateDto cleanDto = normalize(dto, null);
        return validateReferences(cleanDto)
                .then(repository.hasConflictingRate(
                        cleanDto.locationId(), cleanDto.vehicleTypeId(), cleanDto.reservationClassId(),
                        cleanDto.billingTypeId(), cleanDto.effectiveFrom(), cleanDto.effectiveTo(), null))
                .flatMap(conflict -> conflict
                        ? Mono.error(new IllegalStateException(
                                "A conflicting active pricing rule already exists for this combination and time range."))
                        : repository.create(cleanDto));
    }

    public Flux<PricingRateDto> getAllPricingRates() {
        return repository.findAll();
    }

    public Mono<PricingRateDto> getPricingRateById(Long id) {
        return repository.findById(id);
    }

    public Mono<Integer> updatePricingRate(Long id, PricingRateDto dto) {
        PricingRateDto cleanDto = normalize(dto, id);
        return validateReferences(cleanDto)
                .then(repository.hasConflictingRate(
                        cleanDto.locationId(), cleanDto.vehicleTypeId(), cleanDto.reservationClassId(),
                        cleanDto.billingTypeId(), cleanDto.effectiveFrom(), cleanDto.effectiveTo(), id))
                .flatMap(conflict -> conflict
                        ? Mono.error(new IllegalStateException(
                                "A conflicting active pricing rule already exists for this combination and time range."))
                        : repository.update(id, cleanDto));
    }

    public Mono<Integer> deletePricingRate(Long id) {
        return repository.delete(id);
    }

    public Mono<PricingRateDto> resolveRate(
            String locationId,
            Integer vehicleTypeId,
            Integer reservationClassId,
            Integer billingTypeId,
            LocalDateTime effectiveAt) {

        if (effectiveAt == null) {
            effectiveAt = LocalDateTime.now();
        }

        return repository.findEffectiveRate(locationId, vehicleTypeId, reservationClassId, billingTypeId, effectiveAt);
    }

    private Mono<Void> validateReferences(PricingRateDto dto) {
        return locationRepository.findById(dto.locationId())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Location not found.")))
                .flatMap(location -> {
                    if (!"ACTIVE".equalsIgnoreCase(location.status())) {
                        return Mono.error(new IllegalStateException("Location is inactive."));
                    }
                    return vehicleTypeRepository.findById(dto.vehicleTypeId())
                            .switchIfEmpty(Mono.error(new IllegalArgumentException("Vehicle type not found.")))
                            .then(reservationClassRepository.findById(dto.reservationClassId())
                                    .switchIfEmpty(
                                            Mono.error(new IllegalArgumentException("Reservation class not found.")))
                                    .then(billingTypeService.getBillingTypeById(dto.billingTypeId().longValue())
                                            .switchIfEmpty(
                                                    Mono.error(new IllegalArgumentException("Billing type not found.")))
                                            .then()));
                });
    }

    private PricingRateDto normalize(PricingRateDto dto, Long id) {
        if (dto.locationId() == null || dto.locationId().isBlank()) {
            throw new IllegalArgumentException("Location ID is required.");
        }
        if (dto.vehicleTypeId() == null || dto.reservationClassId() == null || dto.billingTypeId() == null) {
            throw new IllegalArgumentException("Vehicle type, reservation class, and billing type are required.");
        }
        if (dto.baseRate().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Base rate must be greater than zero.");
        }
        if (dto.currency().isBlank() || dto.currency().trim().length() != 3) {
            throw new IllegalArgumentException("Currency must be a 3-letter ISO code.");
        }
        if (dto.effectiveFrom() == null) {
            throw new IllegalArgumentException("Effective from timestamp is required.");
        }
        if (dto.effectiveTo() != null && dto.effectiveTo().isBefore(dto.effectiveFrom())) {
            throw new IllegalArgumentException("Effective to cannot be before effective from.");
        }

        return new PricingRateDto(
                id != null ? id : dto.rateId(),
                dto.locationId().trim(),
                dto.vehicleTypeId(),
                dto.reservationClassId(),
                dto.billingTypeId(),
                dto.baseRate(),
                dto.currency().trim().toUpperCase(),
                dto.effectiveFrom(),
                dto.effectiveTo(),
                dto.isActive() == null ? Boolean.TRUE : dto.isActive(),
                dto.createdAt() == null ? LocalDateTime.now() : dto.createdAt(),
                LocalDateTime.now());
    }
}
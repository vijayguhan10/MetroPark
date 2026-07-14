package com.example.Metropark.payments.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.payments.dto.PricingRateDto;
import com.example.Metropark.payments.repo.PricingRateRepository;
import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.repo.VehicleTypeRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PricingRateServiceTest {

    @Mock
    private PricingRateRepository repository;

    @Mock
    private LocationRepository locationRepository;

    @Mock
    private VehicleTypeRepository vehicleTypeRepository;

    @InjectMocks
    private PricingRateService service;

    @Test
    void createPricingRateNormalizesAndDelegates() {
        PricingRateDto input = new PricingRateDto(
                null,
                " LOC-1 ",
                2,
                new java.math.BigDecimal("10.00"),
                " usd ",
                LocalDateTime.of(2024, 1, 15, 10, 30),
                null,
                null,
                null,
                null);

        when(locationRepository.findById("LOC-1")).thenReturn(Mono.just(new LocationDto("LOC-1", 1, "Central Lot", "Metro City", "ACTIVE")));
        when(vehicleTypeRepository.findById(2)).thenReturn(Mono.just(new VehicleTypeDto(2, "Sedan")));
        when(repository.hasConflictingRate("LOC-1", 2, input.effectiveFrom(), null, null)).thenReturn(Mono.just(false));
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createPricingRate(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<PricingRateDto> captor = ArgumentCaptor.forClass(PricingRateDto.class);
        verify(repository).create(captor.capture());

        PricingRateDto saved = captor.getValue();
        assertEquals("LOC-1", saved.locationId());
        assertEquals("USD", saved.currency());
        assertEquals(Boolean.TRUE, saved.isActive());
        assertNotNull(saved.createdAt());
        assertNotNull(saved.updatedAt());
    }

    @Test
    void createPricingRateRejectsConflicts() {
        PricingRateDto input = new PricingRateDto(
                null,
                "LOC-1",
                2,
                new java.math.BigDecimal("10.00"),
                "USD",
                LocalDateTime.of(2024, 1, 15, 10, 30),
                null,
                null,
                null,
                null);

        when(locationRepository.findById("LOC-1")).thenReturn(Mono.just(new LocationDto("LOC-1", 1, "Central Lot", "Metro City", "ACTIVE")));
        when(vehicleTypeRepository.findById(2)).thenReturn(Mono.just(new VehicleTypeDto(2, "Sedan")));
        when(repository.hasConflictingRate("LOC-1", 2, input.effectiveFrom(), null, null)).thenReturn(Mono.just(true));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.createPricingRate(input).block());

        assertEquals("A conflicting active pricing rule already exists for this combination and time range.",
                exception.getMessage());
    }

    @Test
    void resolveRateUsesCurrentTimeWhenNotProvided() {
        when(repository.findEffectiveRate(eq("LOC-1"), eq(2), any())).thenReturn(Mono.just(new PricingRateDto(
                1L,
                "LOC-1",
                2,
                new java.math.BigDecimal("10.00"),
                "USD",
                LocalDateTime.of(2024, 1, 15, 10, 30),
                null,
                true,
                LocalDateTime.of(2024, 1, 15, 10, 30),
                LocalDateTime.of(2024, 1, 15, 10, 30))));

        PricingRateDto response = service.resolveRate("LOC-1", 2, null).block();

        assertEquals("LOC-1", response.locationId());
        ArgumentCaptor<LocalDateTime> captor = ArgumentCaptor.forClass(LocalDateTime.class);
        verify(repository).findEffectiveRate(eq("LOC-1"), eq(2), captor.capture());
        assertNotNull(captor.getValue());
    }
}

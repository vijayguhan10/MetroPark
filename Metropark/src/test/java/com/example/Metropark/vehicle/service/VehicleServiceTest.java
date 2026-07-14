package com.example.Metropark.vehicle.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.repo.VehicleRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class VehicleServiceTest {

    @Mock
    private VehicleRepository repository;

    @InjectMocks
    private VehicleService service;

    @Test
    void registerVehicleTrimsAndNormalizesFields() {
        VehicleDto input = new VehicleDto(
                null,
                "USR-1",
                " ab 123 -cd ",
                2,
                " Toyota ",
                " Corolla ",
                " Blue ",
                null,
                null,
                null);

        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.registerVehicle(input).block();

        assertEquals(1, rows);

        ArgumentCaptor<VehicleDto> captor = ArgumentCaptor.forClass(VehicleDto.class);
        verify(repository).create(captor.capture());

        VehicleDto saved = captor.getValue();
        assertEquals("USR-1", saved.userId());
        assertEquals("AB123CD", saved.vehicleNumber());
        assertEquals("Toyota", saved.brand());
        assertEquals("Corolla", saved.model());
        assertEquals("Blue", saved.color());
        assertTrue(saved.isActive());
        assertEquals(saved.createdAt(), saved.updatedAt());
        assertFalse(saved.vehicleNumber().contains(" "));
    }

    @Test
    void registerVehicleRejectsMissingIdentifiers() {
        VehicleDto input = new VehicleDto(null, null, "ABC123", null, null, null, null, null, null, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.registerVehicle(input).block());

        assertEquals("User ID and Vehicle Type ID are required.", exception.getMessage());
    }

    @Test
    void toggleVehicleStatusReturnsRows() {
        when(repository.updateActiveStatus(1, true)).thenReturn(Mono.just(1));

        Integer rows = service.toggleVehicleStatus(1, true).block();

        assertEquals(1, rows);
        verify(repository).updateActiveStatus(1, true);
    }

    @Test
    void toggleVehicleStatusRejectsNullStatus() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.toggleVehicleStatus(1, null).block());

        assertEquals("Status (isActive) must be true or false.", exception.getMessage());
    }

    @Test
    void toggleVehicleStatusFailsWhenNoRowsAreUpdated() {
        when(repository.updateActiveStatus(1, false)).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.toggleVehicleStatus(1, false).block());

        assertEquals("Vehicle not found.", exception.getMessage());
    }
}

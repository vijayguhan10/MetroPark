package com.example.Metropark.vehicle.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.repo.VehicleTypeRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class VehicleTypeServiceTest {

    @Mock
    private VehicleTypeRepository repository;

    @InjectMocks
    private VehicleTypeService service;

    @Test
    void createVehicleTypeTrimsDisplayName() {
        VehicleTypeDto input = new VehicleTypeDto(null, "  sedan  ");
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createVehicleType(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<VehicleTypeDto> captor = ArgumentCaptor.forClass(VehicleTypeDto.class);
        verify(repository).create(captor.capture());
        assertEquals("sedan", captor.getValue().typeDisplayName());
    }

    @Test
    void createVehicleTypeRejectsBlankName() {
        VehicleTypeDto input = new VehicleTypeDto(null, "   ");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createVehicleType(input).block());

        assertEquals("Vehicle type display name cannot be empty.", exception.getMessage());
    }

    @Test
    void updateVehicleTypeTrimsDisplayName() {
        VehicleTypeDto input = new VehicleTypeDto(null, "  hatchback  ");
        when(repository.update(7, new VehicleTypeDto(null, "hatchback"))).thenReturn(Mono.just(1));

        Integer rows = service.updateVehicleType(7, input).block();

        assertEquals(1, rows);
        verify(repository).update(7, new VehicleTypeDto(null, "hatchback"));
    }

    @Test
    void deleteVehicleTypeDelegatesToRepository() {
        when(repository.delete(3)).thenReturn(Mono.just(1));

        Integer rows = service.deleteVehicleType(3).block();

        assertEquals(1, rows);
        verify(repository).delete(3);
    }
}

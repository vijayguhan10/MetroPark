package com.example.Metropark.parking.service;

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

import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ParkingSlotServiceTest {

    @Mock
    private ParkingSlotRepository repository;

    @InjectMocks
    private ParkingSlotService service;

    @Test
    void createSlotNormalizesFields() {
        ParkingSlotDto input = new ParkingSlotDto(null, "LOC-1", " a-01 ", 2, 3, " SENSOR-1 ", null);
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createSlot(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<ParkingSlotDto> captor = ArgumentCaptor.forClass(ParkingSlotDto.class);
        verify(repository).create(captor.capture());

        ParkingSlotDto saved = captor.getValue();
        assertEquals("A-01", saved.displayCode());
        assertEquals("SENSOR-1", saved.sensorId());
        assertEquals("AVAILABLE", saved.currentStatus());
    }

    @Test
    void createSlotRejectsMissingRequiredFields() {
        ParkingSlotDto input = new ParkingSlotDto(null, null, "A-01", 2, 3, "SENSOR-1", null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createSlot(input).block());

        assertEquals("Location ID, Display Code, and Sensor ID are required.", exception.getMessage());
    }

    @Test
    void updateSlotStatusReturnsRows() {
        when(repository.updateStatus(1, "OCCUPIED")).thenReturn(Mono.just(1));

        Integer rows = service.updateSlotStatus(1, "occupied").block();

        assertEquals(1, rows);
        verify(repository).updateStatus(1, "OCCUPIED");
    }

    @Test
    void updateSlotStatusFailsWhenNotFound() {
        when(repository.updateStatus(1, "OCCUPIED")).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updateSlotStatus(1, "occupied").block());

        assertEquals("Update failed: Slot not found or status unchanged.", exception.getMessage());
    }
}

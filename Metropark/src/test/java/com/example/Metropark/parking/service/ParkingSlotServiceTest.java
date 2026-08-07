package com.example.Metropark.parking.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.event.EventPublisher;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ParkingSlotServiceTest {

    @Mock
    private ParkingSlotRepository repository;

    @Mock
    private RedisStateService redisStateService;

    @Mock
    private EventPublisher eventPublisher;

    @InjectMocks
    private ParkingSlotService service;

    @Test
    void createSlotPersistsSavedDtoWithGeneratedId() {
        ParkingSlotDto input = new ParkingSlotDto(null, "LOC-1", " a-01 ", 2, 3, " SENSOR-1 ", null);
        when(repository.create(any())).thenReturn(Mono.just(1));
        when(redisStateService.saveSlot(any(), anyLong())).thenReturn(Mono.empty());
        when(redisStateService.incrementVersion(eq("slot"), anyString())).thenReturn(Mono.just(1L));
        when(eventPublisher.publishSlotCreated(any(), anyLong())).thenReturn(Mono.empty());

        Integer slotId = service.createSlot(input).block();

        assertEquals(1, slotId);
        ArgumentCaptor<ParkingSlotDto> captor = ArgumentCaptor.forClass(ParkingSlotDto.class);
        verify(redisStateService).saveSlot(captor.capture(), anyLong());

        ParkingSlotDto saved = captor.getValue();
        assertEquals(1, saved.slotId());
        assertEquals("A-01", saved.displayCode());
        assertEquals("SENSOR-1", saved.sensorId());
        assertEquals("AVAILABLE", saved.currentStatus());

        ArgumentCaptor<SlotEventPayload> payloadCaptor = ArgumentCaptor.forClass(SlotEventPayload.class);
        verify(eventPublisher).publishSlotCreated(payloadCaptor.capture(), anyLong());
        assertEquals(1, payloadCaptor.getValue().slotId());
        assertEquals("AVAILABLE", payloadCaptor.getValue().currentStatus());
    }

    @Test
    void createSlotRejectsMissingRequiredFields() {
        ParkingSlotDto input = new ParkingSlotDto(null, null, "A-01", 2, 3, "SENSOR-1", null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createSlot(input).block());

        assertEquals("Location ID, Display Code, and Sensor ID are required.", exception.getMessage());
    }

    @Test
    void createSlotsReturnsCountOfCreatedSlots() {
        ParkingSlotDto first = new ParkingSlotDto(null, "LOC-1", "A-01", 2, 3, "SENSOR-1", null);
        ParkingSlotDto second = new ParkingSlotDto(null, "LOC-1", "A-02", 2, 3, "SENSOR-2", null);

        when(repository.create(any()))
                .thenReturn(Mono.just(11))
                .thenReturn(Mono.just(12));
        when(redisStateService.saveSlot(any(), anyLong())).thenReturn(Mono.empty());
        when(redisStateService.incrementVersion(eq("slot"), anyString())).thenReturn(Mono.just(1L));
        when(eventPublisher.publishSlotCreated(any(), anyLong())).thenReturn(Mono.empty());

        Integer created = service.createSlots(List.of(first, second)).block();

        assertEquals(2, created);
    }

    @Test
    void updateSlotStatusReturnsRows() {
        when(repository.updateStatus(1, "OCCUPIED")).thenReturn(Mono.just(1));
        when(redisStateService.incrementVersion(eq("slot"), eq("1"))).thenReturn(Mono.just(7L));
        when(redisStateService.updateSlotStatus(1, "OCCUPIED", 7L)).thenReturn(Mono.empty());
        when(eventPublisher.publishSlotUpdated(any(), eq(7L))).thenReturn(Mono.empty());

        Integer rows = service.updateSlotStatus(1, "occupied").block();

        assertEquals(1, rows);
        verify(repository).updateStatus(1, "OCCUPIED");
    }

    @Test
    void updateSlotStatusFailsWhenNotFound() {
        when(repository.updateStatus(1, "OCCUPIED")).thenReturn(Mono.just(0));
        when(redisStateService.incrementVersion(eq("slot"), eq("1"))).thenReturn(Mono.just(7L));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updateSlotStatus(1, "occupied").block());

        assertEquals("Update failed: Slot not found or status unchanged.", exception.getMessage());
    }
}

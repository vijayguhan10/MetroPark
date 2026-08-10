package com.example.Metropark.parking.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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

    @InjectMocks
    private ParkingSlotService service;

    @Test
    void createSlotPersistsSavedDtoWithGeneratedId() {

        ParkingSlotDto input = new ParkingSlotDto(
                null,
                "LOC-1",
                " a-01 ",
                2,
                3,
                " SENSOR-1 ",
                null);

        when(repository.create(any(ParkingSlotDto.class)))
                .thenReturn(Mono.just(1));

        when(redisStateService.saveSlotState(
                any(SlotEventPayload.class),
                anyLong())).thenReturn(Mono.just(
                        new SlotEventPayload(
                                1,
                                "LOC-1",
                                "A-01",
                                2,
                                3,
                                "SENSOR-1",
                                "AVAILABLE",
                                null)));

        Integer slotId = service.createSlot(input).block();

        assertEquals(1, slotId);

        ArgumentCaptor<SlotEventPayload> captor = ArgumentCaptor.forClass(SlotEventPayload.class);

        verify(redisStateService)
                .saveSlotState(
                        captor.capture(),
                        anyLong());

        SlotEventPayload saved = captor.getValue();

        assertEquals(1, saved.slotId());
        assertEquals("LOC-1", saved.locationId());
        assertEquals("A-01", saved.displayCode());
        assertEquals("SENSOR-1", saved.sensorId());
        assertEquals("AVAILABLE", saved.currentStatus());
    }

    @Test
    void createSlotRejectsMissingRequiredFields() {

        ParkingSlotDto input = new ParkingSlotDto(
                null,
                null,
                "A-01",
                2,
                3,
                "SENSOR-1",
                null);

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> service.createSlot(input).block());

        assertEquals(
                "Location ID, Display Code, and Sensor ID are required.",
                exception.getMessage());
    }

    @Test
    void createSlotsReturnsCountOfCreatedSlots() {

        ParkingSlotDto first = new ParkingSlotDto(
                null,
                "LOC-1",
                "A-01",
                2,
                3,
                "SENSOR-1",
                null);

        ParkingSlotDto second = new ParkingSlotDto(
                null,
                "LOC-1",
                "A-02",
                2,
                3,
                "SENSOR-2",
                null);

        when(repository.create(any(ParkingSlotDto.class)))
                .thenReturn(Mono.just(11))
                .thenReturn(Mono.just(12));

        when(redisStateService.saveSlotState(
                any(SlotEventPayload.class),
                anyLong())).thenReturn(Mono.just(
                        new SlotEventPayload(
                                11,
                                "LOC-1",
                                "A-01",
                                2,
                                3,
                                "SENSOR-1",
                                "AVAILABLE",
                                null)));

        Integer created = service.createSlots(
                List.of(first, second)).block();

        assertEquals(2, created);
    }

    @Test
    void updateSlotStatusReturnsRows() {

        SlotEventPayload existingSlot = new SlotEventPayload(
                10,
                "LOC-1",
                "A-01",
                2,
                3,
                "SENSOR-1",
                "AVAILABLE",
                java.time.LocalDateTime.now());

        when(redisStateService.getSlot(anyInt()))
                .thenReturn(Mono.just(existingSlot));

        when(redisStateService.incrementVersion(
                eq("slot"),
                eq("10"))).thenReturn(Mono.just(7L));

        when(redisStateService.saveSlotState(
                any(SlotEventPayload.class),
                eq(7L))).thenReturn(Mono.just(existingSlot));

        Integer rows = service.updateSlotStatus(
                10,
                "occupied").block();

        assertEquals(1, rows);
    }

    @Test
    void updateSlotStatusFailsWhenNotFound() {

        when(redisStateService.getSlot(99))
                .thenReturn(Mono.empty());

        when(repository.findById(99))
                .thenReturn(Mono.empty());

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> service.updateSlotStatus(
                        99,
                        "occupied").block());

        assertEquals(
                "Update failed: Slot not found.",
                exception.getMessage());
    }
}
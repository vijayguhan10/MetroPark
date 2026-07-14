package com.example.Metropark.parking.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.service.ParkingSlotService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ParkingSlotControllerTest {

    @Mock
    private ParkingSlotService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ParkingSlotController(service)).build();
    }

    @Test
    void createParkingSlotReturnsCreatedMessage() {
        when(service.createSlot(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/parking-slots")
                .bodyValue(TestFixtures.parkingSlotDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Parking slot registered successfully.", response);
    }

    @Test
    void getAllParkingSlotsReturnsList() {
        when(service.getAllSlots()).thenReturn(Flux.just(TestFixtures.parkingSlotDto()));

        List<ParkingSlotDto> response = webTestClient.get()
                .uri("/api/parking-slots")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ParkingSlotDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.parkingSlotDto()), response);
    }

    @Test
    void getParkingSlotByIdReturnsSlot() {
        when(service.getSlotById(1)).thenReturn(Mono.just(TestFixtures.parkingSlotDto()));

        ParkingSlotDto response = webTestClient.get()
                .uri("/api/parking-slots/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(ParkingSlotDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.parkingSlotDto(), response);
    }
}

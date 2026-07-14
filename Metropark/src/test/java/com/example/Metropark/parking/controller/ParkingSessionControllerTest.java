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

import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.service.ParkingSessionService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ParkingSessionControllerTest {

    @Mock
    private ParkingSessionService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ParkingSessionController(service)).build();
    }

    @Test
    void createParkingSessionReturnsCreatedMessage() {
        when(service.createSession(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/parking-sessions")
                .bodyValue(TestFixtures.parkingSessionDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Parking session initiated successfully.", response);
    }

    @Test
    void getAllParkingSessionsReturnsList() {
        when(service.getAllSessions()).thenReturn(Flux.just(TestFixtures.parkingSessionDto()));

        List<ParkingSessionDto> response = webTestClient.get()
                .uri("/api/parking-sessions")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ParkingSessionDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.parkingSessionDto()), response);
    }

    @Test
    void getParkingSessionByIdReturnsSession() {
        when(service.getSessionById(1)).thenReturn(Mono.just(TestFixtures.parkingSessionDto()));

        ParkingSessionDto response = webTestClient.get()
                .uri("/api/parking-sessions/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(ParkingSessionDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.parkingSessionDto(), response);
    }

    @Test
    void updateParkingSessionStatusReturnsSuccessMessage() {
        when(service.updateSessionStatus(1, "active", 1)).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/parking-sessions/1/status?status=active&currentVersion=1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Parking session status updated successfully.", response);
    }
}

package com.example.Metropark.reservation.controller;

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

import com.example.Metropark.reservation.dto.ReservationDto;
import com.example.Metropark.reservation.service.ReservationService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ReservationControllerTest {

    @Mock
    private ReservationService reservationService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ReservationController(reservationService)).build();
    }

    @Test
    void createReservationReturnsCreatedMessage() {
        when(reservationService.createReservation(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/reservations")
                .bodyValue(TestFixtures.reservationDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Reservation placed successfully.", response);
    }

    @Test
    void getAllReservationsReturnsList() {
        when(reservationService.getAllReservations()).thenReturn(Flux.just(TestFixtures.reservationDto()));

        List<ReservationDto> response = webTestClient.get()
                .uri("/api/reservations")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ReservationDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.reservationDto()), response);
    }

    @Test
    void getReservationByIdReturnsReservation() {
        when(reservationService.getReservationById(1)).thenReturn(Mono.just(TestFixtures.reservationDto()));

        ReservationDto response = webTestClient.get()
                .uri("/api/reservations/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(ReservationDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.reservationDto(), response);
    }

    @Test
    void updateReservationStatusReturnsSuccessMessage() {
        when(reservationService.updateStatus(1, "confirmed", 1)).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/reservations/1/status?status=confirmed&currentVersion=1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Reservation status updated.", response);
    }
}

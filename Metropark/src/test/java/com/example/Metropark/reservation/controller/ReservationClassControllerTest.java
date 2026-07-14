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

import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.service.ReservationClassService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ReservationClassControllerTest {

    @Mock
    private ReservationClassService reservationClassService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new ReservationClassController(reservationClassService)).build();
    }

    @Test
    void createReservationClassReturnsCreatedMessage() {
        when(reservationClassService.createReservationClass(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/reservation-classes")
                .bodyValue(TestFixtures.reservationClassDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Reservation class created successfully.", response);
    }

    @Test
    void getAllReservationClassesReturnsList() {
        when(reservationClassService.getAllReservationClasses()).thenReturn(Flux.just(TestFixtures.reservationClassDto()));

        List<ReservationClassDto> response = webTestClient.get()
                .uri("/api/reservation-classes")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(ReservationClassDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.reservationClassDto()), response);
    }

    @Test
    void getReservationClassByIdReturnsReservationClass() {
        when(reservationClassService.getReservationClassById(1)).thenReturn(Mono.just(TestFixtures.reservationClassDto()));

        ReservationClassDto response = webTestClient.get()
                .uri("/api/reservation-classes/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(ReservationClassDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.reservationClassDto(), response);
    }

    @Test
    void updateReservationClassReturnsSuccessMessage() {
        when(reservationClassService.updateReservationClass(1, TestFixtures.reservationClassDto())).thenReturn(Mono.just(1));

        String response = webTestClient.put()
                .uri("/api/reservation-classes/1")
                .bodyValue(TestFixtures.reservationClassDto())
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Reservation class updated successfully.", response);
    }

    @Test
    void deleteReservationClassReturnsSuccessMessage() {
        when(reservationClassService.deleteReservationClass(1)).thenReturn(Mono.just(1));

        String response = webTestClient.delete()
                .uri("/api/reservation-classes/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Reservation class deleted successfully.", response);
    }
}

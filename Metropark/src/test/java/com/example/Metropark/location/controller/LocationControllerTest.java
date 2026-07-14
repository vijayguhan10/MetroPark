package com.example.Metropark.location.controller;

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

import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.service.LocationService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class LocationControllerTest {

    @Mock
    private LocationService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new LocationController(service)).build();
    }

    @Test
    void createLocationReturnsCreatedMessage() {
        when(service.createLocation(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/locations")
                .bodyValue(TestFixtures.locationDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Location created successfully.", response);
    }

    @Test
    void getAllLocationsReturnsList() {
        when(service.getAllLocations()).thenReturn(Flux.just(TestFixtures.locationDto()));

        List<LocationDto> response = webTestClient.get()
                .uri("/api/locations")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(LocationDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.locationDto()), response);
    }

    @Test
    void getLocationByIdReturnsLocation() {
        when(service.getLocationById("LOC-1")).thenReturn(Mono.just(TestFixtures.locationDto()));

        LocationDto response = webTestClient.get()
                .uri("/api/locations/LOC-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(LocationDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.locationDto(), response);
    }

    @Test
    void updateLocationStatusReturnsSuccessMessage() {
        when(service.updateLocationStatus("LOC-1", "INACTIVE")).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/locations/LOC-1/status?status=inactive")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Location status updated successfully.", response);
    }
}

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

import com.example.Metropark.location.dto.LocationTypeDto;
import com.example.Metropark.location.service.LocationTypeService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class LocationTypeControllerTest {

    @Mock
    private LocationTypeService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new LocationTypeController(service)).build();
    }

    @Test
    void createLocationTypeReturnsCreatedMessage() {
        when(service.createLocationType(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/location-types")
                .bodyValue(TestFixtures.locationTypeDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Location type created successfully.", response);
    }

    @Test
    void getAllLocationTypesReturnsList() {
        when(service.getAllLocationTypes()).thenReturn(Flux.just(TestFixtures.locationTypeDto()));

        List<LocationTypeDto> response = webTestClient.get()
                .uri("/api/location-types")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(LocationTypeDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.locationTypeDto()), response);
    }

    @Test
    void getLocationTypeByIdReturnsType() {
        when(service.getLocationTypeById(1)).thenReturn(Mono.just(TestFixtures.locationTypeDto()));

        LocationTypeDto response = webTestClient.get()
                .uri("/api/location-types/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(LocationTypeDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.locationTypeDto(), response);
    }

    @Test
    void updateLocationTypeReturnsSuccessMessage() {
        when(service.updateLocationType(1, TestFixtures.locationTypeDto())).thenReturn(Mono.just(1));

        String response = webTestClient.put()
                .uri("/api/location-types/1")
                .bodyValue(TestFixtures.locationTypeDto())
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Location type updated successfully.", response);
    }

    @Test
    void deleteLocationTypeReturnsSuccessMessage() {
        when(service.deleteLocationType(1)).thenReturn(Mono.just(1));

        String response = webTestClient.delete()
                .uri("/api/location-types/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Location type deleted successfully.", response);
    }
}

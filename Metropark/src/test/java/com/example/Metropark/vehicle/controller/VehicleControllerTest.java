package com.example.Metropark.vehicle.controller;

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

import com.example.Metropark.testsupport.TestFixtures;
import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.service.VehicleService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class VehicleControllerTest {

    @Mock
    private VehicleService vehicleService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new VehicleController(vehicleService)).build();
    }

    @Test
    void createVehicleReturnsCreatedMessage() {
        when(vehicleService.registerVehicle(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/vehicles")
                .bodyValue(TestFixtures.vehicleDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Vehicle registered successfully.", response);
    }

    @Test
    void createVehicleReturnsBadRequestOnValidationError() {
        when(vehicleService.registerVehicle(any()))
                .thenReturn(Mono.error(new IllegalArgumentException("Vehicle registration number is required.")));

        String response = webTestClient.post()
                .uri("/api/vehicles")
                .bodyValue(TestFixtures.vehicleDto())
                .exchange()
                .expectStatus().isBadRequest()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Vehicle registration number is required.", response);
    }

    @Test
    void getAllVehiclesReturnsList() {
        when(vehicleService.getAllVehicles()).thenReturn(Flux.just(TestFixtures.vehicleDto()));

        List<VehicleDto> response = webTestClient.get()
                .uri("/api/vehicles")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(VehicleDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.vehicleDto()), response);
    }

    @Test
    void getVehicleByIdReturnsVehicle() {
        when(vehicleService.getVehicleById(1)).thenReturn(Mono.just(TestFixtures.vehicleDto()));

        VehicleDto response = webTestClient.get()
                .uri("/api/vehicles/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(VehicleDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.vehicleDto(), response);
    }

    @Test
    void getVehicleByIdReturnsNotFoundWhenMissing() {
        when(vehicleService.getVehicleById(99)).thenReturn(Mono.empty());

        webTestClient.get()
                .uri("/api/vehicles/99")
                .exchange()
                .expectStatus().isNotFound();
    }

    @Test
    void updateVehicleStatusReturnsSuccessMessage() {
        when(vehicleService.toggleVehicleStatus(1, true)).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/vehicles/1/status?isActive=true")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Vehicle status updated successfully.", response);
    }
}

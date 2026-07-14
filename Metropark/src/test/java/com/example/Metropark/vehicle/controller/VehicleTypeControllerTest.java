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
import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.service.VehicleTypeService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class VehicleTypeControllerTest {

    @Mock
    private VehicleTypeService vehicleTypeService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new VehicleTypeController(vehicleTypeService)).build();
    }

    @Test
    void createVehicleTypeReturnsCreatedMessage() {
        when(vehicleTypeService.createVehicleType(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/vehicle-types")
                .bodyValue(TestFixtures.vehicleTypeDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Vehicle type created successfully.", response);
    }

    @Test
    void getAllVehicleTypesReturnsList() {
        when(vehicleTypeService.getAllVehicleTypes()).thenReturn(Flux.just(TestFixtures.vehicleTypeDto()));

        List<VehicleTypeDto> response = webTestClient.get()
                .uri("/api/vehicle-types")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(VehicleTypeDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.vehicleTypeDto()), response);
    }

    @Test
    void getVehicleTypeByIdReturnsVehicleType() {
        when(vehicleTypeService.getVehicleTypeById(1)).thenReturn(Mono.just(TestFixtures.vehicleTypeDto()));

        VehicleTypeDto response = webTestClient.get()
                .uri("/api/vehicle-types/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(VehicleTypeDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.vehicleTypeDto(), response);
    }

    @Test
    void updateVehicleTypeReturnsNotFoundWhenNoRowsAreUpdated() {
        when(vehicleTypeService.updateVehicleType(1, TestFixtures.vehicleTypeDto())).thenReturn(Mono.just(0));

        String response = webTestClient.put()
                .uri("/api/vehicle-types/1")
                .bodyValue(TestFixtures.vehicleTypeDto())
                .exchange()
                .expectStatus().isNotFound()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Vehicle type not found", response);
    }

    @Test
    void deleteVehicleTypeReturnsSuccessMessage() {
        when(vehicleTypeService.deleteVehicleType(1)).thenReturn(Mono.just(1));

        String response = webTestClient.delete()
                .uri("/api/vehicle-types/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Vehicle type deleted successfully.", response);
    }
}

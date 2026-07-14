package com.example.Metropark.gate.controller;

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

import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.gate.service.GateService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class GateControllerTest {

    @Mock
    private GateService gateService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new GateController(gateService)).build();
    }

    @Test
    void createGateReturnsCreatedMessage() {
        when(gateService.createGate(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/gates")
                .bodyValue(TestFixtures.gateDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Gate created successfully.", response);
    }

    @Test
    void getAllGatesReturnsList() {
        when(gateService.getAllGates()).thenReturn(Flux.just(TestFixtures.gateDto()));

        List<GateDto> response = webTestClient.get()
                .uri("/api/gates")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(GateDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.gateDto()), response);
    }

    @Test
    void getGateByIdReturnsGate() {
        when(gateService.getGateById(1)).thenReturn(Mono.just(TestFixtures.gateDto()));

        GateDto response = webTestClient.get()
                .uri("/api/gates/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(GateDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.gateDto(), response);
    }

    @Test
    void updateGateStatusReturnsSuccessMessage() {
        when(gateService.updateGateStatus(1, "active")).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/gates/1/status?status=active")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Gate status updated successfully.", response);
    }
}

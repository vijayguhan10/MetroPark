package com.example.Metropark.payments.controller;

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

import com.example.Metropark.payments.dto.PricingRateDto;
import com.example.Metropark.payments.service.PricingRateService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PricingRateControllerTest {

    @Mock
    private PricingRateService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new PricingRateController(service)).build();
    }

    @Test
    void createPricingRateReturnsCreatedMessage() {
        when(service.createPricingRate(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/pricing-rates")
                .bodyValue(TestFixtures.pricingRateDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Pricing rate created successfully.", response);
    }

    @Test
    void getAllPricingRatesReturnsList() {
        when(service.getAllPricingRates()).thenReturn(Flux.just(TestFixtures.pricingRateDto()));

        List<PricingRateDto> response = webTestClient.get()
                .uri("/api/pricing-rates")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(PricingRateDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.pricingRateDto()), response);
    }

    @Test
    void getPricingRateByIdReturnsRate() {
        when(service.getPricingRateById(1L)).thenReturn(Mono.just(TestFixtures.pricingRateDto()));

        PricingRateDto response = webTestClient.get()
                .uri("/api/pricing-rates/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(PricingRateDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.pricingRateDto(), response);
    }

    @Test
    void resolvePricingRateReturnsRate() {
        when(service.resolveRate("LOC-1", 2, null)).thenReturn(Mono.just(TestFixtures.pricingRateDto()));

        PricingRateDto response = webTestClient.get()
                .uri("/api/pricing-rates/resolve?locationId=LOC-1&vehicleTypeId=2")
                .exchange()
                .expectStatus().isOk()
                .expectBody(PricingRateDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.pricingRateDto(), response);
    }

    @Test
    void updatePricingRateReturnsSuccessMessage() {
        when(service.updatePricingRate(1L, TestFixtures.pricingRateDto())).thenReturn(Mono.just(1));

        String response = webTestClient.put()
                .uri("/api/pricing-rates/1")
                .bodyValue(TestFixtures.pricingRateDto())
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Pricing rate updated successfully.", response);
    }

    @Test
    void deletePricingRateReturnsSuccessMessage() {
        when(service.deletePricingRate(1L)).thenReturn(Mono.just(1));

        String response = webTestClient.delete()
                .uri("/api/pricing-rates/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Pricing rate deleted successfully.", response);
    }
}

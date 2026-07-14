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

import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.service.PaymentMethodService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PaymentMethodControllerTest {

    @Mock
    private PaymentMethodService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new PaymentMethodController(service)).build();
    }

    @Test
    void createPaymentMethodReturnsCreatedMessage() {
        when(service.createPaymentMethod(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/payment-methods")
                .bodyValue(TestFixtures.paymentMethodDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Payment method created successfully.", response);
    }

    @Test
    void getAllPaymentMethodsReturnsList() {
        when(service.getAllPaymentMethods()).thenReturn(Flux.just(TestFixtures.paymentMethodDto()));

        List<PaymentMethodDto> response = webTestClient.get()
                .uri("/api/payment-methods")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(PaymentMethodDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.paymentMethodDto()), response);
    }

    @Test
    void getPaymentMethodByIdReturnsMethod() {
        when(service.getPaymentMethodById(1L)).thenReturn(Mono.just(TestFixtures.paymentMethodDto()));

        PaymentMethodDto response = webTestClient.get()
                .uri("/api/payment-methods/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(PaymentMethodDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.paymentMethodDto(), response);
    }

    @Test
    void updatePaymentMethodReturnsSuccessMessage() {
        when(service.updatePaymentMethod(1L, TestFixtures.paymentMethodDto())).thenReturn(Mono.just(1));

        String response = webTestClient.put()
                .uri("/api/payment-methods/1")
                .bodyValue(TestFixtures.paymentMethodDto())
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Payment method updated successfully.", response);
    }

    @Test
    void deletePaymentMethodReturnsSuccessMessage() {
        when(service.deletePaymentMethod(1L)).thenReturn(Mono.just(1));

        String response = webTestClient.delete()
                .uri("/api/payment-methods/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Payment method deleted successfully.", response);
    }
}

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

import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.service.PaymentService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PaymentControllerTest {

    @Mock
    private PaymentService service;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new PaymentController(service)).build();
    }

    @Test
    void createPaymentReturnsCreatedMessage() {
        when(service.createPayment(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/payments")
                .bodyValue(TestFixtures.paymentDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Payment created successfully.", response);
    }

    @Test
    void getAllPaymentsReturnsList() {
        when(service.getAllPayments()).thenReturn(Flux.just(TestFixtures.paymentDto()));

        List<PaymentDto> response = webTestClient.get()
                .uri("/api/payments")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(PaymentDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.paymentDto()), response);
    }

    @Test
    void getPaymentByIdReturnsPayment() {
        when(service.getPaymentById(1L)).thenReturn(Mono.just(TestFixtures.paymentDto()));

        PaymentDto response = webTestClient.get()
                .uri("/api/payments/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(PaymentDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.paymentDto(), response);
    }

    @Test
    void getPaymentByReferenceReturnsPayment() {
        when(service.getPaymentByTransactionReference("TX-100")).thenReturn(Mono.just(TestFixtures.paymentDto()));

        PaymentDto response = webTestClient.get()
                .uri("/api/payments/reference/TX-100")
                .exchange()
                .expectStatus().isOk()
                .expectBody(PaymentDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.paymentDto(), response);
    }

    @Test
    void getPaymentsBySessionIdReturnsList() {
        when(service.getPaymentsBySessionId(10)).thenReturn(Flux.just(TestFixtures.paymentDto()));

        List<PaymentDto> response = webTestClient.get()
                .uri("/api/payments/session/10")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(PaymentDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.paymentDto()), response);
    }

    @Test
    void updatePaymentStatusReturnsSuccessMessage() {
        when(service.updatePaymentStatus(1L, TestFixtures.paymentStatusUpdateDto())).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/payments/1/status")
                .bodyValue(TestFixtures.paymentStatusUpdateDto())
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Payment status updated successfully.", response);
    }
}

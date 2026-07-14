package com.example.Metropark.queue.controller;

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

import com.example.Metropark.queue.dto.QueueDto;
import com.example.Metropark.queue.service.QueueService;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class QueueControllerTest {

    @Mock
    private QueueService queueService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new QueueController(queueService)).build();
    }

    @Test
    void createQueueReturnsCreatedMessage() {
        when(queueService.createQueue(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/queues")
                .bodyValue(TestFixtures.queueDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Queue successfully created.", response);
    }

    @Test
    void getAllQueuesReturnsList() {
        when(queueService.getAllQueues()).thenReturn(Flux.just(TestFixtures.queueDto()));

        List<QueueDto> response = webTestClient.get()
                .uri("/api/queues")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(QueueDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.queueDto()), response);
    }

    @Test
    void getQueueByIdReturnsQueue() {
        when(queueService.getQueueById(1)).thenReturn(Mono.just(TestFixtures.queueDto()));

        QueueDto response = webTestClient.get()
                .uri("/api/queues/1")
                .exchange()
                .expectStatus().isOk()
                .expectBody(QueueDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.queueDto(), response);
    }

    @Test
    void updateQueueStatusReturnsSuccessMessage() {
        when(queueService.updateQueueStatus(1, "active")).thenReturn(Mono.just(1));

        String response = webTestClient.patch()
                .uri("/api/queues/1/status?status=active")
                .exchange()
                .expectStatus().isOk()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("Queue status updated.", response);
    }
}

package com.example.Metropark.event.consumer;

import java.time.Instant;

import org.junit.jupiter.api.Test;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;

import com.example.Metropark.event.Event;
import com.example.Metropark.event.payload.CameraEventPayload;
import com.example.Metropark.redis.RedisStateService;
import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Mono;

class EventConsumerTest {

    private final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();

    @Test
    void consumeCameraEventShouldParseRawEventEnvelopeAndPersistPayload() throws Exception {
        RedisStateService redisStateService = mock(RedisStateService.class);
        EventConsumer eventConsumer = new EventConsumer(redisStateService, objectMapper);

        CameraEventPayload payload = new CameraEventPayload(
                "KA01AB1234",
                "PARKING-LOT-1",
                "CAM-1",
                Instant.parse("2026-08-06T12:00:00Z"));

        Event event = Event.of("camera.car.entered", "CAM-1", 1L, payload);
        byte[] body = objectMapper.writeValueAsBytes(event);
        Message message = new Message(body, new MessageProperties());

        when(redisStateService.saveCameraEvent(org.mockito.ArgumentMatchers.any(CameraEventPayload.class)))
                .thenReturn(Mono.empty());

        eventConsumer.consumeCameraEvent(message);

        verify(redisStateService, times(1)).saveCameraEvent(org.mockito.ArgumentMatchers.any(CameraEventPayload.class));
    }
}

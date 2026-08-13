package com.example.Metropark.camera.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.example.Metropark.camera.repo.CameraEventRepository;
import com.example.Metropark.config.RabbitMQConfig;

import reactor.core.publisher.Mono;

@Service
public class CameraEventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(CameraEventPublisher.class);

    private final RabbitTemplate rabbitTemplate;
    private final CameraEventRepository cameraEventRepository;

    public CameraEventPublisher(RabbitTemplate rabbitTemplate, CameraEventRepository cameraEventRepository) {
        this.rabbitTemplate = rabbitTemplate;
        this.cameraEventRepository = cameraEventRepository;
    }

    public Mono<CameraEvent> recordAndPublish(CameraEvent event) {
        return cameraEventRepository.save(event)
                .then(Mono.fromRunnable(() -> rabbitTemplate.convertAndSend(
                        RabbitMQConfig.CAMERA_EVENTS_EXCHANGE,
                        event.eventType().routingKey(),
                        event)))
                .thenReturn(event)
                .doOnSuccess(published -> LOGGER.debug(
                        "CAMERA {} | plate={} lot={} camera={} event={}",
                        published.eventType(), published.licensePlate(), published.parkingLotId(),
                        published.cameraId(), published.eventId()))
                .doOnError(error -> LOGGER.error(
                        "Failed to record or publish camera event {} for plate {}",
                        event.eventId(), event.licensePlate(), error));
    }
}

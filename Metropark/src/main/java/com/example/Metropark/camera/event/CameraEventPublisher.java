package com.example.Metropark.camera.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.example.Metropark.camera.repo.CameraEventRepository;
import com.example.Metropark.config.RabbitMQConfig;

import reactor.core.publisher.Mono;

/**
 * Makes a camera observation durable, then announces it.
 *
 * <p>
 * The order is the contract, which is why persisting and publishing live behind
 * one method instead of being left to each caller: a row written first and never
 * published is a visible RECEIVED event an operator can replay, whereas an event
 * published first and never written is a car that parked with no record of what
 * let it in. Callers that did their own sequencing would eventually get it
 * backwards.
 *
 * <p>
 * The event is published as-is rather than wrapped in
 * {@link com.example.Metropark.event.Event}: {@link CameraEvent} already carries
 * its own id, type and timestamp, so wrapping would duplicate all three and give
 * consumers two ids to disagree about.
 */
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

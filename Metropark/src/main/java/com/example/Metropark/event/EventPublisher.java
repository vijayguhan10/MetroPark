package com.example.Metropark.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.springframework.amqp.rabbit.core.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.payload.CameraEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.ReservationEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;

import reactor.core.publisher.Mono;

@Service
public class EventPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventPublisher.class);

    private final RabbitTemplate rabbitTemplate;

    public EventPublisher(RabbitTemplate rabbitTemplate) {
        this.rabbitTemplate = rabbitTemplate;
    }

    public Mono<Void> publishSlotCreated(SlotEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.SLOT_CREATED_KEY, payload.slotId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.SLOT_CREATED_KEY, event);
    }

    public Mono<Void> publishSlotUpdated(SlotEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.SLOT_UPDATED_KEY, payload.slotId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.SLOT_UPDATED_KEY, event);
    }

    public Mono<Void> publishSlotDeleted(Integer slotId, long version) {
        Event event = Event.of(RabbitMQConfig.SLOT_DELETED_KEY, slotId.toString(), version, slotId);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.SLOT_DELETED_KEY, event);
    }

    public Mono<Void> publishReservationCreated(ReservationEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.RESERVATION_CREATED_KEY, payload.reservationId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.RESERVATION_CREATED_KEY, event);
    }

    public Mono<Void> publishReservationCancelled(ReservationEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.RESERVATION_CANCELLED_KEY, payload.reservationId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.RESERVATION_CANCELLED_KEY, event);
    }

    public Mono<Void> publishSessionStarted(SessionEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.SESSION_STARTED_KEY, payload.sessionId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.SESSION_STARTED_KEY, event);
    }

    public Mono<Void> publishSessionEnded(SessionEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.SESSION_ENDED_KEY, payload.sessionId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.SESSION_ENDED_KEY, event);
    }

    public Mono<Void> publishSessionStatusChanged(SessionEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.SESSION_STATUS_CHANGED_KEY, payload.sessionId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.SESSION_STATUS_CHANGED_KEY, event);
    }

    public Mono<Void> publishPaymentCompleted(PaymentEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.PAYMENT_COMPLETED_KEY, payload.paymentId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.PAYMENT_COMPLETED_KEY, event);
    }

    public Mono<Void> publishPaymentFailed(PaymentEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.PAYMENT_FAILED_KEY, payload.paymentId().toString(), version, payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.PAYMENT_FAILED_KEY, event);
    }

    public Mono<Void> publishCameraCarEntered(CameraEventPayload payload) {
        Event event = Event.of(RabbitMQConfig.CAMERA_CAR_ENTERED_KEY, payload.cameraId(), 1, payload);
        return publish(RabbitMQConfig.CAMERA_EVENTS_EXCHANGE, RabbitMQConfig.CAMERA_CAR_ENTERED_KEY, event);
    }

    public Mono<Void> publishCameraCarExited(CameraEventPayload payload) {
        Event event = Event.of(RabbitMQConfig.CAMERA_CAR_EXITED_KEY, payload.cameraId(), 1, payload);
        return publish(RabbitMQConfig.CAMERA_EVENTS_EXCHANGE, RabbitMQConfig.CAMERA_CAR_EXITED_KEY, event);
    }

    private Mono<Void> publish(String exchange, String routingKey, Event event) {
        return Mono.fromRunnable(() -> {
            rabbitTemplate.convertAndSend(exchange, routingKey, event);
            LOGGER.debug("Published event: {} to exchange: {} with routing key: {}", event.type(), exchange, routingKey);
        }).then();
    }
}
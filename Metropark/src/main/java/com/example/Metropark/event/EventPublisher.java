package com.example.Metropark.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
// import org.springframework.amqp.rabbit.core.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
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

    /**
     * One vehicle entry: session CREATED, slot OCCUPIED, payment PENDING, applied
     * by the consumer in a single PostgreSQL transaction.
     */
    public Mono<Void> publishVehicleEntry(ParkingLifecycleEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.VEHICLE_ENTRY_KEY, payload.session().sessionId().toString(), version,
                payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.VEHICLE_ENTRY_KEY, event);
    }

    /**
     * One vehicle exit: session EXITED, slot AVAILABLE, payment SUCCESS, applied by
     * the consumer in a single PostgreSQL transaction.
     */
    public Mono<Void> publishVehicleExit(ParkingLifecycleEventPayload payload, long version) {
        Event event = Event.of(RabbitMQConfig.VEHICLE_EXIT_KEY, payload.session().sessionId().toString(), version,
                payload);
        return publish(RabbitMQConfig.PARKING_EVENTS_EXCHANGE, RabbitMQConfig.VEHICLE_EXIT_KEY, event);
    }

    // Camera events are NOT published here. They carry their own envelope
    // (com.example.Metropark.camera.event.CameraEvent) and go out through
    // CameraEventPublisher, which persists the observation before announcing it.
    // Wrapping them in Event as well gave every camera event two ids and two
    // timestamps that could disagree.

    private Mono<Void> publish(String exchange, String routingKey, Event event) {
        return Mono.fromRunnable(() -> {
            rabbitTemplate.convertAndSend(exchange, routingKey, event);
            LOGGER.debug("Published event: {} to exchange: {} with routing key: {}", event.type(), exchange, routingKey);
        }).then();
    }
}
package com.example.Metropark.event.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.Event;
import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.ReservationEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.parking.repo.ParkingLifecycleRepository;
import com.example.Metropark.redis.RedisStateService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Mono;

/**
 * Applies published events to the durable stores.
 *
 * <p>
 * The container factory runs in {@code AcknowledgeMode.MANUAL}, so every
 * listener here MUST settle its delivery. Returning without an ack or a nack
 * leaves the message unacknowledged, and once prefetch is exhausted the queue
 * stops delivering entirely - the simulation would appear to run while nothing
 * reached PostgreSQL. Each listener therefore acks only after its reactive work
 * has completed, and nacks (to the dead letter queue, since
 * {@code defaultRequeueRejected} is false) on failure.
 */
@Component
public class EventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private final RedisStateService redisStateService;
    private final ParkingLifecycleRepository lifecycleRepository;
    private final ObjectMapper objectMapper;

    public EventConsumer(
            RedisStateService redisStateService,
            ParkingLifecycleRepository lifecycleRepository,
            ObjectMapper objectMapper) {

        this.redisStateService = redisStateService;
        this.lifecycleRepository = lifecycleRepository;
        this.objectMapper = objectMapper;
    }

    /**
     * The only listener that writes PostgreSQL. One vehicle entry or exit becomes
     * one transaction across parking_sessions, parking_slots and payments, and the
     * delivery is acknowledged only once that transaction has committed.
     */
    @RabbitListener(queues = RabbitMQConfig.LIFECYCLE_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumeLifecycleEvent(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable lifecycle envelope");
            return;
        }

        ParkingLifecycleEventPayload payload = parsePayload(event.payload(), ParkingLifecycleEventPayload.class);
        if (payload == null || payload.session() == null) {
            deadLetter(channel, deliveryTag, "lifecycle event " + event.eventId() + " carries no session");
            return;
        }

        Integer sessionId = payload.session().sessionId();

        Mono<ParkingLifecycleRepository.PersistResult> work = switch (event.type()) {
            case RabbitMQConfig.VEHICLE_ENTRY_KEY -> lifecycleRepository.persistEntry(payload);
            case RabbitMQConfig.VEHICLE_EXIT_KEY -> lifecycleRepository.persistExit(payload);
            default -> {
                LOGGER.warn("Ignoring unknown lifecycle event type {}", event.type());
                yield Mono.empty();
            }
        };

        work.subscribe(
                result -> LOGGER.info(
                        "PERSISTED {} | session={} sessionRows={} slotRows={} paymentRows={}",
                        event.type(), sessionId, result.sessionRows(), result.slotRows(), result.paymentRows()),
                error -> {
                    LOGGER.error("Failed to persist {} for session {}", event.type(), sessionId, error);
                    deadLetter(channel, deliveryTag, "persist failed for session " + sessionId);
                },
                () -> ack(channel, deliveryTag));
    }

    @RabbitListener(queues = RabbitMQConfig.SLOT_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumeSlotEvent(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable slot envelope");
            return;
        }

        long eventVersion = event.version();

        switch (event.type()) {
            case RabbitMQConfig.SLOT_CREATED_KEY, RabbitMQConfig.SLOT_UPDATED_KEY -> {
                SlotEventPayload payload = parsePayload(event.payload(), SlotEventPayload.class);
                if (payload == null) {
                    deadLetter(channel, deliveryTag, "slot event " + event.eventId() + " carries no payload");
                    return;
                }
                settle(
                        redisStateService.getSlotVersion(payload.slotId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.saveSlotState(payload, eventVersion)),
                        channel, deliveryTag, "slot " + payload.slotId());
            }
            case RabbitMQConfig.SLOT_DELETED_KEY -> {
                Integer slotId = parsePayload(event.payload(), Integer.class);
                if (slotId == null) {
                    deadLetter(channel, deliveryTag, "slot delete event " + event.eventId() + " carries no id");
                    return;
                }
                settle(
                        redisStateService.getSlotVersion(slotId)
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.deleteSlot(slotId)),
                        channel, deliveryTag, "slot delete " + slotId);
            }
            default -> ack(channel, deliveryTag);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.RESERVATION_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumeReservationEvent(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable reservation envelope");
            return;
        }

        long eventVersion = event.version();
        ReservationEventPayload payload = parsePayload(event.payload(), ReservationEventPayload.class);
        if (payload == null) {
            deadLetter(channel, deliveryTag, "reservation event " + event.eventId() + " carries no payload");
            return;
        }

        switch (event.type()) {
            case RabbitMQConfig.RESERVATION_CREATED_KEY -> settle(
                    redisStateService.getReservationVersion(payload.reservationId())
                            .filter(currentVersion -> currentVersion < eventVersion)
                            .flatMap(v -> redisStateService.saveReservation(toReservationDto(payload), eventVersion)),
                    channel, deliveryTag, "reservation " + payload.reservationId());
            case RabbitMQConfig.RESERVATION_CANCELLED_KEY -> settle(
                    redisStateService.getReservationVersion(payload.reservationId())
                            .filter(currentVersion -> currentVersion < eventVersion)
                            .flatMap(v -> redisStateService.deleteReservation(payload.reservationId())),
                    channel, deliveryTag, "reservation cancel " + payload.reservationId());
            default -> ack(channel, deliveryTag);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.SESSION_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumeSessionEvent(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable session envelope");
            return;
        }

        long eventVersion = event.version();
        SessionEventPayload payload = parsePayload(event.payload(), SessionEventPayload.class);
        if (payload == null) {
            deadLetter(channel, deliveryTag, "session event " + event.eventId() + " carries no payload");
            return;
        }

        switch (event.type()) {
            case RabbitMQConfig.SESSION_STARTED_KEY, RabbitMQConfig.SESSION_STATUS_CHANGED_KEY -> settle(
                    redisStateService.getSessionVersion(payload.sessionId())
                            .filter(currentVersion -> currentVersion < eventVersion)
                            .flatMap(v -> redisStateService.saveSessionState(payload, eventVersion)),
                    channel, deliveryTag, "session " + payload.sessionId());
            case RabbitMQConfig.SESSION_ENDED_KEY -> settle(
                    redisStateService.getSessionVersion(payload.sessionId())
                            .filter(currentVersion -> currentVersion < eventVersion)
                            .flatMap(v -> redisStateService.deleteSession(payload.sessionId())),
                    channel, deliveryTag, "session end " + payload.sessionId());
            default -> ack(channel, deliveryTag);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.PAYMENT_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumePaymentEvent(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable payment envelope");
            return;
        }

        long eventVersion = event.version();
        PaymentEventPayload payload = parsePayload(event.payload(), PaymentEventPayload.class);
        if (payload == null) {
            deadLetter(channel, deliveryTag, "payment event " + event.eventId() + " carries no payload");
            return;
        }

        switch (event.type()) {
            case RabbitMQConfig.PAYMENT_COMPLETED_KEY, RabbitMQConfig.PAYMENT_FAILED_KEY -> settle(
                    redisStateService.getPaymentVersion(payload.paymentId())
                            .filter(currentVersion -> currentVersion < eventVersion)
                            .flatMap(v -> redisStateService.savePaymentState(payload, eventVersion)),
                    channel, deliveryTag, "payment " + payload.paymentId());
            default -> ack(channel, deliveryTag);
        }
    }

    // Camera events are no longer consumed here. They are read from the
    // parking-camera-events topic exchange by ParkingCameraConsumer (which drives
    // parking) and CameraEventAuditConsumer (which maintains camera_events.status).
    // This listener only wrote them to a Redis list, which nothing read.

    /**
     * Acks once {@code work} completes, dead letters if it fails. A filtered-out
     * (stale-version) event completes empty and is still acked - it was handled,
     * just not applied.
     */
    private void settle(Mono<?> work, Channel channel, long deliveryTag, String what) {
        work.subscribe(
                result -> LOGGER.debug("Synced {} to Redis", what),
                error -> {
                    LOGGER.error("Error syncing {} to Redis", what, error);
                    deadLetter(channel, deliveryTag, what);
                },
                () -> ack(channel, deliveryTag));
    }

    private void ack(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOGGER.error("Failed to ack delivery {}", deliveryTag, e);
        }
    }

    private void deadLetter(Channel channel, long deliveryTag, String what) {
        LOGGER.error("Dead lettering delivery {} ({})", deliveryTag, what);
        try {
            // requeue=false: the broker routes to the queue's configured dead letter
            // queue. Requeueing instead would spin the same poison message forever.
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            LOGGER.error("Failed to nack delivery {}", deliveryTag, e);
        }
    }

    private Event parseEvent(Message message) {
        try {
            return objectMapper.readValue(message.getBody(), Event.class);
        } catch (Exception e) {
            LOGGER.error("Failed to parse event from message", e);
            return null;
        }
    }

    private <T> T parsePayload(Object payload, Class<T> clazz) {
        try {
            if (payload == null) {
                return null;
            }
            if (clazz.isInstance(payload)) {
                return clazz.cast(payload);
            }
            return objectMapper.convertValue(payload, clazz);
        } catch (Exception e) {
            LOGGER.error("Failed to parse payload as {}", clazz.getSimpleName(), e);
            return null;
        }
    }

    private com.example.Metropark.reservation.dto.ReservationDto toReservationDto(ReservationEventPayload payload) {
        return new com.example.Metropark.reservation.dto.ReservationDto(
                payload.reservationId(),
                payload.userId(),
                payload.slotId(),
                payload.queueEntryId(),
                payload.reservationStatus(),
                payload.reservationVersion(),
                payload.reservedAt(),
                payload.expiresAt(),
                payload.updatedAt(),
                payload.updatedAt());
    }
}

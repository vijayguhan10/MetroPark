package com.example.Metropark.event.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.Event;
import com.example.Metropark.event.payload.CameraEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.ReservationEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Mono;

@Component
public class EventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private final RedisStateService redisStateService;

    public EventConsumer(RedisStateService redisStateService) {
        this.redisStateService = redisStateService;
    }

    @RabbitListener(queues = RabbitMQConfig.SLOT_EVENTS_QUEUE)
    public void consumeSlotEvent(Message message) {
        try {
            Event event = parseEvent(message);
            if (event == null) return;

            long eventVersion = event.version();
            
            switch (event.type()) {
                case RabbitMQConfig.SLOT_CREATED_KEY, RabbitMQConfig.SLOT_UPDATED_KEY -> {
                    SlotEventPayload payload = parsePayload(event.payload(), SlotEventPayload.class);
                    if (payload != null) {
                        redisStateService.getSlotVersion(payload.slotId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.saveSlot(toSlotDto(payload), eventVersion))
                                .subscribe(
                                        unused -> LOGGER.info("Synced slot {} to Redis from event {}", payload.slotId(), event.eventId()),
                                        e -> LOGGER.error("Error syncing slot {} from event {}", payload.slotId(), event.eventId(), e)
                                );
                    }
                }
                case RabbitMQConfig.SLOT_DELETED_KEY -> {
                    Integer slotId = (Integer) event.payload();
                    redisStateService.getSlotVersion(slotId)
                            .filter(currentVersion -> currentVersion < eventVersion)
                            .flatMap(v -> redisStateService.deleteSlot(slotId))
                            .subscribe(
                                    unused -> LOGGER.info("Deleted slot {} from Redis from event {}", slotId, event.eventId()),
                                    e -> LOGGER.error("Error deleting slot {} from Redis from event {}", slotId, event.eventId(), e)
                            );
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing slot event", e);
            throw new RuntimeException("Failed to process slot event", e);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.RESERVATION_EVENTS_QUEUE)
    public void consumeReservationEvent(Message message) {
        try {
            Event event = parseEvent(message);
            if (event == null) return;

            long eventVersion = event.version();
            
            switch (event.type()) {
                case RabbitMQConfig.RESERVATION_CREATED_KEY -> {
                    ReservationEventPayload payload = parsePayload(event.payload(), ReservationEventPayload.class);
                    if (payload != null) {
                        redisStateService.getReservationVersion(payload.reservationId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.saveReservation(toReservationDto(payload), eventVersion))
                                .subscribe(
                                        unused -> LOGGER.info("Synced reservation {} to Redis from event {}", payload.reservationId(), event.eventId()),
                                        e -> LOGGER.error("Error syncing reservation {} from event {}", payload.reservationId(), event.eventId(), e)
                                );
                    }
                }
                case RabbitMQConfig.RESERVATION_CANCELLED_KEY -> {
                    ReservationEventPayload payload = parsePayload(event.payload(), ReservationEventPayload.class);
                    if (payload != null) {
                        redisStateService.getReservationVersion(payload.reservationId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.deleteReservation(payload.reservationId()))
                                .subscribe(
                                        unused -> LOGGER.info("Deleted reservation {} from Redis from event {}", payload.reservationId(), event.eventId()),
                                        e -> LOGGER.error("Error deleting reservation {} from Redis from event {}", payload.reservationId(), event.eventId(), e)
                                );
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing reservation event", e);
            throw new RuntimeException("Failed to process reservation event", e);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.SESSION_EVENTS_QUEUE)
    public void consumeSessionEvent(Message message) {
        try {
            Event event = parseEvent(message);
            if (event == null) return;

            long eventVersion = event.version();
            
            switch (event.type()) {
                case RabbitMQConfig.SESSION_STARTED_KEY, RabbitMQConfig.SESSION_STATUS_CHANGED_KEY -> {
                    SessionEventPayload payload = parsePayload(event.payload(), SessionEventPayload.class);
                    if (payload != null) {
                        redisStateService.getSessionVersion(payload.sessionId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.saveSession(toSessionDto(payload), eventVersion))
                                .subscribe(
                                        unused -> LOGGER.info("Synced session {} to Redis from event {}", payload.sessionId(), event.eventId()),
                                        e -> LOGGER.error("Error syncing session {} from event {}", payload.sessionId(), event.eventId(), e)
                                );
                    }
                }
                case RabbitMQConfig.SESSION_ENDED_KEY -> {
                    SessionEventPayload payload = parsePayload(event.payload(), SessionEventPayload.class);
                    if (payload != null) {
                        redisStateService.getSessionVersion(payload.sessionId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.deleteSession(payload.sessionId()))
                                .subscribe(
                                        unused -> LOGGER.info("Deleted session {} from Redis from event {}", payload.sessionId(), event.eventId()),
                                        e -> LOGGER.error("Error deleting session {} from Redis from event {}", payload.sessionId(), event.eventId(), e)
                                );
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing session event", e);
            throw new RuntimeException("Failed to process session event", e);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.PAYMENT_EVENTS_QUEUE)
    public void consumePaymentEvent(Message message) {
        try {
            Event event = parseEvent(message);
            if (event == null) return;

            long eventVersion = event.version();
            
            switch (event.type()) {
                case RabbitMQConfig.PAYMENT_COMPLETED_KEY, RabbitMQConfig.PAYMENT_FAILED_KEY -> {
                    PaymentEventPayload payload = parsePayload(event.payload(), PaymentEventPayload.class);
                    if (payload != null) {
                        redisStateService.getPaymentVersion(payload.paymentId())
                                .filter(currentVersion -> currentVersion < eventVersion)
                                .flatMap(v -> redisStateService.savePayment(toPaymentDto(payload), eventVersion))
                                .subscribe(
                                        unused -> LOGGER.info("Synced payment {} to Redis from event {}", payload.paymentId(), event.eventId()),
                                        e -> LOGGER.error("Error syncing payment {} from event {}", payload.paymentId(), event.eventId(), e)
                                );
                    }
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error processing payment event", e);
            throw new RuntimeException("Failed to process payment event", e);
        }
    }

    @RabbitListener(queues = RabbitMQConfig.CAMERA_EVENTS_QUEUE)
    public void consumeCameraEvent(Message message) {
        try {
            Event event = parseEvent(message);
            if (event == null) return;

            CameraEventPayload payload = parsePayload(event.payload(), CameraEventPayload.class);
            if (payload != null) {
                redisStateService.saveCameraEvent(payload)
                        .subscribe(
                                unused -> LOGGER.info("Saved camera event to Redis from event {}", event.eventId()),
                                e -> LOGGER.error("Error saving camera event from event {}", event.eventId(), e)
                        );
            }
        } catch (Exception e) {
            LOGGER.error("Error processing camera event", e);
            throw new RuntimeException("Failed to process camera event", e);
        }
    }

    private Event parseEvent(Message message) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(message.getBody(), Event.class);
        } catch (Exception e) {
            LOGGER.error("Failed to parse event from message", e);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T parsePayload(Object payload, Class<T> clazz) {
        try {
            if (payload instanceof java.util.Map) {
                return new com.fasterxml.jackson.databind.ObjectMapper().convertValue(payload, clazz);
            }
            return clazz.cast(payload);
        } catch (Exception e) {
            LOGGER.error("Failed to parse payload", e);
            return null;
        }
    }

    // Helper methods to convert payloads to DTOs
    private com.example.Metropark.parking.dto.ParkingSlotDto toSlotDto(SlotEventPayload payload) {
        return new com.example.Metropark.parking.dto.ParkingSlotDto(
                payload.slotId(),
                payload.locationId(),
                payload.displayCode(),
                payload.vehicleTypeId(),
                payload.reservationClassId(),
                payload.sensorId(),
                payload.currentStatus()
        );
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
                payload.updatedAt()
        );
    }

    private com.example.Metropark.parking.dto.ParkingSessionDto toSessionDto(SessionEventPayload payload) {
        return new com.example.Metropark.parking.dto.ParkingSessionDto(
                payload.sessionId(),
                payload.reservationId(),
                payload.slotId(),
                payload.userId(),
                payload.vehicleId(),
                payload.entryGateId(),
                payload.exitGateId(),
                payload.sessionStatus(),
                payload.actualEntryTime(),
                payload.actualExitTime(),
                payload.expectedExitTime(),
                payload.durationMinutes(),
                payload.paymentStatus(),
                payload.sessionVersion(),
                payload.updatedAt(),
                payload.updatedAt()
        );
    }

    private com.example.Metropark.payments.dto.PaymentDto toPaymentDto(PaymentEventPayload payload) {
        return new com.example.Metropark.payments.dto.PaymentDto(
                payload.paymentId(),
                payload.transactionReference(),
                payload.sessionId(),
                payload.userId(),
                payload.methodId(),
                payload.amount(),
                payload.currency(),
                payload.paymentStatus(),
                payload.gatewayResponseCode(),
                payload.gatewayResponseMessage(),
                payload.processedAt(),
                payload.updatedAt(),
                payload.updatedAt()
        );
    }
}
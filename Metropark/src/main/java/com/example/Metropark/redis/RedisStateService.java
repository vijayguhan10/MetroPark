package com.example.Metropark.redis;

import java.time.Duration;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.stereotype.Service;

import com.example.Metropark.event.payload.CameraEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.ReservationEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.reservation.dto.ReservationDto;

import com.fasterxml.jackson.databind.ObjectMapper;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class RedisStateService {

        private static final Logger LOGGER = LoggerFactory.getLogger(RedisStateService.class);

        private final ReactiveRedisTemplate<String, Object> redisTemplate;
        private final ReactiveValueOperations<String, Object> valueOps;
        private final ObjectMapper objectMapper;

        // Key prefixes
        private static final String SLOT_PREFIX = "parking:slot:";
        private static final String RESERVATION_PREFIX = "parking:reservation:";
        private static final String SESSION_PREFIX = "parking:session:";
        private static final String PAYMENT_PREFIX = "parking:payment:";
        private static final String OCCUPANCY_PREFIX = "parking:occupancy:";
        private static final String VERSION_PREFIX = "parking:version:";
        private static final String CAMERA_EVENTS_KEY = "parking:camera:events";

        // TTL for keys (24 hours)
        private static final Duration DEFAULT_TTL = Duration.ofHours(24);

        public RedisStateService(ReactiveRedisTemplate<String, Object> redisTemplate, ObjectMapper objectMapper) {
                this.redisTemplate = redisTemplate;
                this.valueOps = redisTemplate.opsForValue();
                this.objectMapper = objectMapper;
        }

        // ==================== Parking Slot Operations ====================

        public Mono<Void> saveSlot(ParkingSlotDto slot, long version) {
                String key = SLOT_PREFIX + slot.slotId();
                String versionKey = VERSION_PREFIX + "slot:" + slot.slotId();

                SlotEventPayload payload = new SlotEventPayload(
                                slot.slotId(),
                                slot.locationId(),
                                slot.displayCode(),
                                slot.vehicleTypeId(),
                                slot.reservationClassId(),
                                slot.sensorId(),
                                slot.currentStatus(),
                                java.time.LocalDateTime.now());

                return valueOps.set(key, payload, DEFAULT_TTL)
                                .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Saved slot {} to Redis with version {}", slot.slotId(),
                                                version))
                                .doOnError(e -> LOGGER.error("Error saving slot {} to Redis", slot.slotId(), e));
        }

        public Mono<SlotEventPayload> getSlot(Integer slotId) {
                String key = SLOT_PREFIX + slotId;
                return valueOps.get(key)
                                .map(this::toSlotEventPayload)
                                .doOnError(e -> LOGGER.error("Error getting slot {} from Redis", slotId, e));
        }

        public Mono<Long> getSlotVersion(Integer slotId) {
                String versionKey = VERSION_PREFIX + "slot:" + slotId;
                return valueOps.get(versionKey)
                                .cast(Long.class)
                                .defaultIfEmpty(0L)
                                .doOnError(e -> LOGGER.error("Error getting slot version {} from Redis", slotId, e));
        }

        public Mono<Void> updateSlotStatus(Integer slotId, String status, long version) {
                String key = SLOT_PREFIX + slotId;
                String versionKey = VERSION_PREFIX + "slot:" + slotId;

                return valueOps.get(key)
                                .cast(SlotEventPayload.class)
                                .flatMap(existing -> {
                                        SlotEventPayload updated = new SlotEventPayload(
                                                        existing.slotId(),
                                                        existing.locationId(),
                                                        existing.displayCode(),
                                                        existing.vehicleTypeId(),
                                                        existing.reservationClassId(),
                                                        existing.sensorId(),
                                                        status,
                                                        java.time.LocalDateTime.now());
                                        return valueOps.set(key, updated, DEFAULT_TTL)
                                                        .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                                        .then();
                                })
                                .doOnSuccess(v -> LOGGER.debug("Updated slot {} status to {} with version {}", slotId,
                                                status, version))
                                .doOnError(e -> LOGGER.error("Error updating slot {} status in Redis", slotId, e));
        }

        public Mono<Void> deleteSlot(Integer slotId) {
                String key = SLOT_PREFIX + slotId;
                String versionKey = VERSION_PREFIX + "slot:" + slotId;

                return redisTemplate.delete(key)
                                .then(redisTemplate.delete(versionKey))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Deleted slot {} from Redis", slotId))
                                .doOnError(e -> LOGGER.error("Error deleting slot {} from Redis", slotId, e));
        }

        public Flux<SlotEventPayload> getAllSlots() {
                return redisTemplate.keys(SLOT_PREFIX + "*")
                                .flatMap(key -> valueOps.get(key).map(this::toSlotEventPayload))
                                .doOnError(e -> LOGGER.error("Error getting all slots from Redis", e));
        }

        // ==================== Reservation Operations ====================

        public Mono<Void> saveReservation(ReservationDto reservation, long version) {
                String key = RESERVATION_PREFIX + reservation.reservationId();
                String versionKey = VERSION_PREFIX + "reservation:" + reservation.reservationId();

                ReservationEventPayload payload = new ReservationEventPayload(
                                reservation.reservationId(),
                                reservation.userId(),
                                reservation.slotId(),
                                reservation.queueEntryId(),
                                reservation.reservationStatus(),
                                reservation.reservationVersion(),
                                reservation.reservedAt(),
                                reservation.expiresAt(),
                                java.time.LocalDateTime.now());

                return valueOps.set(key, payload, DEFAULT_TTL)
                                .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Saved reservation {} to Redis with version {}",
                                                reservation.reservationId(), version))
                                .doOnError(e -> LOGGER.error("Error saving reservation {} to Redis",
                                                reservation.reservationId(), e));
        }

        public Mono<ReservationEventPayload> getReservation(Integer reservationId) {
                String key = RESERVATION_PREFIX + reservationId;
                return valueOps.get(key)
                                .map(this::toReservationEventPayload)
                                .doOnError(e -> LOGGER.error("Error getting reservation {} from Redis", reservationId,
                                                e));
        }

        public Mono<Long> getReservationVersion(Integer reservationId) {
                String versionKey = VERSION_PREFIX + "reservation:" + reservationId;
                return valueOps.get(versionKey)
                                .cast(Long.class)
                                .defaultIfEmpty(0L)
                                .doOnError(e -> LOGGER.error("Error getting reservation version {} from Redis",
                                                reservationId, e));
        }

        public Mono<Void> updateReservationStatus(Integer reservationId, String status, long version) {
                String key = RESERVATION_PREFIX + reservationId;
                String versionKey = VERSION_PREFIX + "reservation:" + reservationId;

                return valueOps.get(key)
                                .map(this::toReservationEventPayload)
                                .flatMap(existing -> {
                                        ReservationEventPayload updated = new ReservationEventPayload(
                                                        existing.reservationId(),
                                                        existing.userId(),
                                                        existing.slotId(),
                                                        existing.queueEntryId(),
                                                        status,
                                                        existing.reservationVersion() + 1,
                                                        existing.reservedAt(),
                                                        existing.expiresAt(),
                                                        java.time.LocalDateTime.now());
                                        return valueOps.set(key, updated, DEFAULT_TTL)
                                                        .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                                        .then();
                                })
                                .doOnSuccess(v -> LOGGER.debug("Updated reservation {} status to {} with version {}",
                                                reservationId, status, version))
                                .doOnError(e -> LOGGER.error("Error updating reservation {} status in Redis",
                                                reservationId, e));
        }

        public Mono<Void> deleteReservation(Integer reservationId) {
                String key = RESERVATION_PREFIX + reservationId;
                String versionKey = VERSION_PREFIX + "reservation:" + reservationId;

                return redisTemplate.delete(key)
                                .then(redisTemplate.delete(versionKey))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Deleted reservation {} from Redis", reservationId))
                                .doOnError(e -> LOGGER.error("Error deleting reservation {} from Redis", reservationId,
                                                e));
        }

        public Flux<ReservationEventPayload> getAllReservations() {
                return redisTemplate.keys(RESERVATION_PREFIX + "*")
                                .flatMap(key -> valueOps.get(key).map(this::toReservationEventPayload))
                                .doOnError(e -> LOGGER.error("Error getting all reservations from Redis", e));
        }

        // ==================== Session Operations ====================

        public Mono<Void> saveSession(ParkingSessionDto session, long version) {
                String key = SESSION_PREFIX + session.sessionId();
                String versionKey = VERSION_PREFIX + "session:" + session.sessionId();

                SessionEventPayload payload = new SessionEventPayload(
                                session.sessionId(),
                                session.reservationId(),
                                session.slotId(),
                                session.userId(),
                                session.vehicleId(),
                                session.entryGateId(),
                                session.exitGateId(),
                                session.sessionStatus(),
                                session.actualEntryTime(),
                                session.actualExitTime(),
                                session.expectedExitTime(),
                                session.durationMinutes(),
                                session.paymentStatus(),
                                session.sessionVersion(),
                                java.time.LocalDateTime.now());

                return valueOps.set(key, payload, DEFAULT_TTL)
                                .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Saved session {} to Redis with version {}",
                                                session.sessionId(), version))
                                .doOnError(e -> LOGGER.error("Error saving session {} to Redis", session.sessionId(),
                                                e));
        }

        public Mono<SessionEventPayload> getSession(Integer sessionId) {
                String key = SESSION_PREFIX + sessionId;
                return valueOps.get(key)
                                .map(this::toSessionEventPayload)
                                .doOnError(e -> LOGGER.error("Error getting session {} from Redis", sessionId, e));
        }

        public Mono<Long> getSessionVersion(Integer sessionId) {
                String versionKey = VERSION_PREFIX + "session:" + sessionId;
                return valueOps.get(versionKey)
                                .cast(Long.class)
                                .defaultIfEmpty(0L)
                                .doOnError(e -> LOGGER.error("Error getting session version {} from Redis", sessionId,
                                                e));
        }

        public Mono<Void> updateSessionStatus(Integer sessionId, String status, long version) {
                String key = SESSION_PREFIX + sessionId;
                String versionKey = VERSION_PREFIX + "session:" + sessionId;

                return valueOps.get(key)
                                .map(this::toSessionEventPayload)
                                .flatMap(existing -> {
                                        SessionEventPayload updated = new SessionEventPayload(
                                                        existing.sessionId(),
                                                        existing.reservationId(),
                                                        existing.slotId(),
                                                        existing.userId(),
                                                        existing.vehicleId(),
                                                        existing.entryGateId(),
                                                        existing.exitGateId(),
                                                        status,
                                                        existing.actualEntryTime(),
                                                        existing.actualExitTime(),
                                                        existing.expectedExitTime(),
                                                        existing.durationMinutes(),
                                                        existing.paymentStatus(),
                                                        existing.sessionVersion() + 1,
                                                        java.time.LocalDateTime.now());
                                        return valueOps.set(key, updated, DEFAULT_TTL)
                                                        .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                                        .then();
                                })
                                .doOnSuccess(v -> LOGGER.debug("Updated session {} status to {} with version {}",
                                                sessionId, status, version))
                                .doOnError(e -> LOGGER.error("Error updating session {} status in Redis", sessionId,
                                                e));
        }

        public Mono<Void> deleteSession(Integer sessionId) {
                String key = SESSION_PREFIX + sessionId;
                String versionKey = VERSION_PREFIX + "session:" + sessionId;

                return redisTemplate.delete(key)
                                .then(redisTemplate.delete(versionKey))
                                .doOnSuccess(v -> LOGGER.debug("Deleted session {} from Redis", sessionId))
                                .doOnError(e -> LOGGER.error("Error deleting session {} from Redis", sessionId, e))
                                .then();
        }

        public Flux<SessionEventPayload> getAllSessions() {
                return redisTemplate.keys(SESSION_PREFIX + "*")
                                .flatMap(key -> valueOps.get(key).map(this::toSessionEventPayload))
                                .doOnError(e -> LOGGER.error("Error getting all sessions from Redis", e));
        }

        // ==================== Payment Operations ====================

        public Mono<Void> savePayment(PaymentDto payment, long version) {
                String key = PAYMENT_PREFIX + payment.paymentId();
                String versionKey = VERSION_PREFIX + "payment:" + payment.paymentId();

                PaymentEventPayload payload = new PaymentEventPayload(
                                payment.paymentId(),
                                payment.transactionReference(),
                                payment.sessionId(),
                                payment.userId(),
                                payment.methodId(),
                                payment.amount(),
                                payment.currency(),
                                payment.paymentStatus(),
                                payment.gatewayResponseCode(),
                                payment.gatewayResponseMessage(),
                                payment.processedAt(),
                                java.time.LocalDateTime.now());

                return valueOps.set(key, payload, DEFAULT_TTL)
                                .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Saved payment {} to Redis with version {}",
                                                payment.paymentId(), version))
                                .doOnError(e -> LOGGER.error("Error saving payment {} to Redis", payment.paymentId(),
                                                e));
        }

        public Mono<PaymentEventPayload> getPayment(Long paymentId) {
                String key = PAYMENT_PREFIX + paymentId;
                return valueOps.get(key)
                                .map(this::toPaymentEventPayload)
                                .doOnError(e -> LOGGER.error("Error getting payment {} from Redis", paymentId, e));
        }

        public Mono<Long> getPaymentVersion(Long paymentId) {
                String versionKey = VERSION_PREFIX + "payment:" + paymentId;
                return valueOps.get(versionKey)
                                .cast(Long.class)
                                .defaultIfEmpty(0L)
                                .doOnError(e -> LOGGER.error("Error getting payment version {} from Redis", paymentId,
                                                e));
        }

        public Mono<Void> updatePaymentStatus(Long paymentId, String status, long version) {
                String key = PAYMENT_PREFIX + paymentId;
                String versionKey = VERSION_PREFIX + "payment:" + paymentId;

                return valueOps.get(key)
                                .map(this::toPaymentEventPayload)
                                .flatMap(existing -> {
                                        PaymentEventPayload updated = new PaymentEventPayload(
                                                        existing.paymentId(),
                                                        existing.transactionReference(),
                                                        existing.sessionId(),
                                                        existing.userId(),
                                                        existing.methodId(),
                                                        existing.amount(),
                                                        existing.currency(),
                                                        status,
                                                        existing.gatewayResponseCode(),
                                                        existing.gatewayResponseMessage(),
                                                        existing.processedAt(),
                                                        java.time.LocalDateTime.now());
                                        return valueOps.set(key, updated, DEFAULT_TTL)
                                                        .then(valueOps.set(versionKey, version, DEFAULT_TTL))
                                                        .then();
                                })
                                .doOnSuccess(v -> LOGGER.debug("Updated payment {} status to {} with version {}",
                                                paymentId, status, version))
                                .doOnError(e -> LOGGER.error("Error updating payment {} status in Redis", paymentId,
                                                e));
        }

        public Mono<Void> deletePayment(Long paymentId) {
                String key = PAYMENT_PREFIX + paymentId;
                String versionKey = VERSION_PREFIX + "payment:" + paymentId;

                return redisTemplate.delete(key)
                                .then(redisTemplate.delete(versionKey))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Deleted payment {} from Redis", paymentId))
                                .doOnError(e -> LOGGER.error("Error deleting payment {} from Redis", paymentId, e));
        }

        public Flux<PaymentEventPayload> getAllPayments() {
                return redisTemplate.keys(PAYMENT_PREFIX + "*")
                                .flatMap(key -> valueOps.get(key).map(this::toPaymentEventPayload))
                                .doOnError(e -> LOGGER.error("Error getting all payments from Redis", e));
        }

        private SlotEventPayload toSlotEventPayload(Object value) {
                if (value == null) {
                        return null;
                }
                if (value instanceof SlotEventPayload payload) {
                        return payload;
                }
                return objectMapper.convertValue(value, SlotEventPayload.class);
        }

        private ReservationEventPayload toReservationEventPayload(Object value) {
                if (value == null) {
                        return null;
                }
                if (value instanceof ReservationEventPayload payload) {
                        return payload;
                }
                return objectMapper.convertValue(value, ReservationEventPayload.class);
        }

        private SessionEventPayload toSessionEventPayload(Object value) {
                if (value == null) {
                        return null;
                }
                if (value instanceof SessionEventPayload payload) {
                        return payload;
                }
                return objectMapper.convertValue(value, SessionEventPayload.class);
        }

        private PaymentEventPayload toPaymentEventPayload(Object value) {
                if (value == null) {
                        return null;
                }
                if (value instanceof PaymentEventPayload payload) {
                        return payload;
                }
                return objectMapper.convertValue(value, PaymentEventPayload.class);
        }

        // ==================== Occupancy Operations ====================

        public Mono<Void> updateOccupancy(String locationId, int totalSlots, int occupiedSlots) {
                String key = OCCUPANCY_PREFIX + locationId;
                Map<String, Object> occupancy = Map.of(
                                "locationId", locationId,
                                "totalSlots", totalSlots,
                                "occupiedSlots", occupiedSlots,
                                "availableSlots", totalSlots - occupiedSlots,
                                "occupancyRate", totalSlots > 0 ? (double) occupiedSlots / totalSlots * 100 : 0.0,
                                "updatedAt", java.time.LocalDateTime.now());

                return valueOps.set(key, occupancy, DEFAULT_TTL)
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Updated occupancy for location {}", locationId))
                                .doOnError(e -> LOGGER.error("Error updating occupancy for location {}", locationId,
                                                e));
        }

        @SuppressWarnings("unchecked")
        public Mono<Map<String, Object>> getOccupancy(String locationId) {
                String key = OCCUPANCY_PREFIX + locationId;
                return valueOps.get(key)
                                .cast(Map.class)
                                .map(m -> (Map<String, Object>) m)
                                .doOnError(e -> LOGGER.error("Error getting occupancy for location {}", locationId, e));
        }

        @SuppressWarnings("unchecked")
        public Flux<Map<String, Object>> getAllOccupancies() {
                return redisTemplate.keys(OCCUPANCY_PREFIX + "*")
                                .flatMap(key -> valueOps.get(key).cast(Map.class).map(m -> (Map<String, Object>) m))
                                .doOnError(e -> LOGGER.error("Error getting all occupancies from Redis", e));
        }

        // ==================== Camera Events ====================

        public Mono<Void> saveCameraEvent(CameraEventPayload event) {
                return redisTemplate.opsForList()
                                .rightPush(CAMERA_EVENTS_KEY, event)
                                .then(redisTemplate.expire(CAMERA_EVENTS_KEY, DEFAULT_TTL))
                                .then()
                                .doOnSuccess(v -> LOGGER.debug("Saved camera event to Redis"))
                                .doOnError(e -> LOGGER.error("Error saving camera event to Redis", e));
        }

        public Flux<CameraEventPayload> getRecentCameraEvents(int count) {
                return redisTemplate.opsForList()
                                .range(CAMERA_EVENTS_KEY, -count, -1)
                                .map(this::toCameraEventPayload)
                                .doOnError(e -> LOGGER.error("Error getting recent camera events from Redis", e));
        }

        // ==================== Version Operations ====================

        public Mono<Long> incrementVersion(String entityType, String entityId) {
                String versionKey = VERSION_PREFIX + entityType + ":" + entityId;
                return redisTemplate.opsForValue()
                                .increment(versionKey)
                                .doOnSuccess(v -> LOGGER.debug("Incremented version for {}:{} to {}", entityType,
                                                entityId, v))
                                .doOnError(e -> LOGGER.error("Error incrementing version for {}:{}", entityType,
                                                entityId, e));
        }

        public Mono<Long> getVersion(String entityType, String entityId) {
                String versionKey = VERSION_PREFIX + entityType + ":" + entityId;
                return valueOps.get(versionKey)
                                .cast(Long.class)
                                .defaultIfEmpty(0L)
                                .doOnError(e -> LOGGER.error("Error getting version for {}:{}", entityType, entityId,
                                                e));
        }

        private CameraEventPayload toCameraEventPayload(Object value) {
                if (value == null) {
                        return null;
                }
                if (value instanceof CameraEventPayload payload) {
                        return payload;
                }
                return objectMapper.convertValue(value, CameraEventPayload.class);
        }
}
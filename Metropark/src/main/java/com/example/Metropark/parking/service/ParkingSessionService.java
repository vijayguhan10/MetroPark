package com.example.Metropark.parking.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.event.EventPublisher;
import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSessionResponseDto;
import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.redis.RedisStateService;
import com.example.Metropark.reservation.repo.ReservationRepository;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.repo.VehicleRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ParkingSessionService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSessionService.class);

        private final ParkingSessionRepository sessionRepository;
        private final VehicleRepository vehicleRepository;
        private final ParkingSlotRepository slotRepository;
        private final ReservationRepository reservationRepository;
        private final UserRepository userRepository;
        private final RedisStateService redisStateService;
        private final EventPublisher eventPublisher;

        public ParkingSessionService(
                        ParkingSessionRepository sessionRepository,
                        VehicleRepository vehicleRepository,
                        ParkingSlotRepository slotRepository,
                        ReservationRepository reservationRepository,
                        UserRepository userRepository,
                        RedisStateService redisStateService,
                        EventPublisher eventPublisher) {

                this.sessionRepository = sessionRepository;
                this.vehicleRepository = vehicleRepository;
                this.slotRepository = slotRepository;
                this.reservationRepository = reservationRepository;
                this.userRepository = userRepository;
                this.redisStateService = redisStateService;
                this.eventPublisher = eventPublisher;
        }

        // No @Transactional: this method writes to Redis and RabbitMQ only. It was
        // also a no-op here in any case - the DSLContext is built from the raw R2DBC
        // ConnectionFactory and never joins Spring's reactive transaction.
        public Mono<Integer> createSession(ParkingSessionDto dto) {

                LOGGER.info("Creating parking session: {}", dto);

                if (dto.slotId() == null || dto.userId() == null || dto.vehicleId() == null) {
                        return Mono.error(new IllegalArgumentException(
                                        "Slot ID, User ID, and Vehicle ID are required."));
                }

                Mono<Void> vehicleValidation = vehicleRepository.findById(dto.vehicleId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("Vehicle not found.")))
                                .flatMap(vehicle -> Boolean.TRUE.equals(vehicle.isActive())
                                                ? Mono.<Void>empty()
                                                : Mono.error(new IllegalStateException("Vehicle is inactive.")));

                // Slot availability must be judged against Redis, the real-time store.
                // PostgreSQL still shows the pre-entry status until the consumer catches
                // up, so validating against it would hand the same slot to two vehicles.
                Mono<Void> slotValidation = redisStateService.getSlotStatus(dto.slotId())
                                .switchIfEmpty(slotRepository.findById(dto.slotId())
                                                .switchIfEmpty(Mono.error(new IllegalArgumentException(
                                                                "Parking slot not found.")))
                                                .map(slot -> slot.currentStatus()))
                                .flatMap(currentStatus -> {

                                        if (dto.reservationId() != null) {

                                                if ("RESERVED".equalsIgnoreCase(currentStatus)) {
                                                        return Mono.empty();
                                                }

                                                return Mono.error(new IllegalStateException(
                                                                "Reserved session requires slot status RESERVED."));
                                        }

                                        if ("AVAILABLE".equalsIgnoreCase(currentStatus)) {
                                                return Mono.empty();
                                        }

                                        return Mono.error(new IllegalStateException(
                                                        "Parking slot is " + currentStatus));
                                });

                Mono<Void> userValidation = userRepository.findById(dto.userId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("User not found.")))
                                .flatMap(user -> "ACTIVE".equalsIgnoreCase(user.userStatus())
                                                ? Mono.<Void>empty()
                                                : Mono.error(new IllegalStateException("User account is inactive.")));

                // Same reasoning as the slot check: a session created a moment ago is still
                // in flight to PostgreSQL, so the duplicate check consults Redis first and
                // only falls back to PostgreSQL for sessions predating the current cache.
                Mono<Void> checkDuplicateSession = hasActiveSession(dto.vehicleId())
                                .flatMap(hasSession -> {
                                        if (hasSession) {
                                                return Mono.error(new IllegalStateException(
                                                                "Cannot start session: This vehicle is already parked or has a pending entry in another location."));
                                        }
                                        return Mono.empty();
                                });

                Mono<Void> reservationValidation = Mono.empty();

                if (dto.reservationId() != null) {
                        reservationValidation = reservationRepository.findById(dto.reservationId())
                                        .switchIfEmpty(Mono
                                                        .error(new IllegalArgumentException("Reservation not found.")))
                                        .flatMap(reservation -> {
                                                if ("RESERVED".equalsIgnoreCase(reservation.reservationStatus())) {
                                                        return Mono.empty();
                                                }
                                                return Mono.error(new IllegalStateException(
                                                                "Reservation is " + reservation.reservationStatus()
                                                                                + "."));
                                        });
                }

                return Mono.when(
                                vehicleValidation,
                                slotValidation,
                                userValidation,
                                checkDuplicateSession,
                                reservationValidation)
                                .then(Mono.defer(() -> {

                                        LocalDateTime now = LocalDateTime.now();

                                        ParkingSessionDto session = new ParkingSessionDto(
                                                        dto.sessionId(),
                                                        dto.reservationId(),
                                                        dto.slotId(),
                                                        dto.userId(),
                                                        dto.vehicleId(),
                                                        dto.entryGateId(),
                                                        dto.exitGateId(),
                                                        dto.sessionStatus() == null || dto.sessionStatus().isBlank()
                                                                        ? "CREATED"
                                                                        : dto.sessionStatus().trim().toUpperCase(),
                                                        dto.actualEntryTime(),
                                                        dto.actualExitTime(),
                                                        dto.expectedExitTime(),
                                                        dto.durationMinutes(),
                                                        dto.paymentStatus() == null || dto.paymentStatus().isBlank()
                                                                        ? "PENDING"
                                                                        : dto.paymentStatus().trim().toUpperCase(),
                                                        1,
                                                        now,
                                                        now);

                                        // Business logic -> Redis -> Redis success -> RabbitMQ publish.
                                        // The id is reserved from the PostgreSQL sequence without writing
                                        // a row, so Redis can hold the session before it is persisted and
                                        // the consumer inserts under the very same id.
                                        return sessionRepository.allocateSessionId()
                                                        .flatMap(sessionId -> {
                                                                long version = INITIAL_VERSION;

                                                                SessionEventPayload payload = new SessionEventPayload(
                                                                                sessionId,
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
                                                                                (int) version,
                                                                                now);

                                                                // flatMap (not then) so the publish is built from the
                                                                // state Redis actually stored, and only runs once that
                                                                // write has signalled completion.
                                                                return redisStateService
                                                                                .saveSessionState(payload, version)
                                                                                .flatMap(stored -> eventPublisher
                                                                                                .publishVehicleEntry(
                                                                                                                new ParkingLifecycleEventPayload(
                                                                                                                                stored,
                                                                                                                                null,
                                                                                                                                null,
                                                                                                                                null,
                                                                                                                                stored.sessionVersion()),
                                                                                                                version))
                                                                                .thenReturn(sessionId);
                                                        })
                                                        .doOnSuccess(sessionId -> LOGGER.info(
                                                                        "Parking session created successfully. Session ID: {}, version: {}",
                                                                        sessionId, INITIAL_VERSION))
                                                        .doOnError(error -> LOGGER.error(
                                                                        "Error creating parking session", error));
                                }));
        }


        public Flux<ParkingSessionDto> getAllSessions() {
                LOGGER.debug("Fetching all parking sessions");
                // Try Redis first, fallback to DB
                return redisStateService.getAllSessions()
                                .map(this::toParkingSessionDto)
                                .switchIfEmpty(sessionRepository.findAll())
                                .doOnComplete(() -> LOGGER.debug("Fetched all parking sessions successfully"))
                                .doOnError(e -> LOGGER.error("Error fetching all parking sessions: {}",
                                                e.getMessage()));
        }

        public Mono<ParkingSessionDto> getSessionById(Integer id) {
                LOGGER.debug("Fetching parking session by id: {}", id);
                // Try Redis first, fallback to DB
                return redisStateService.getSession(id)
                                .map(this::toParkingSessionDto)
                                .switchIfEmpty(sessionRepository.findById(id))
                                .doOnSuccess(dto -> LOGGER.debug("Fetched parking session: {}", dto))
                                .doOnError(e -> LOGGER.error("Error fetching parking session by id {}: {}", id,
                                                e.getMessage()));
        }

        public Flux<ParkingSessionResponseDto> getAllSessionsWithDetails() {
                LOGGER.debug("Fetching all parking sessions with details");
                return sessionRepository.findAllWithDetails()
                                .doOnComplete(() -> LOGGER
                                                .debug("Fetched all parking sessions with details successfully"))
                                .doOnError(e -> LOGGER.error("Error fetching all parking sessions with details: {}",
                                                e.getMessage()));
        }

        public Flux<ParkingSessionDto> getAllSessionsFromDb() {
                LOGGER.debug("Fetching all parking sessions directly from DB");
                return sessionRepository.findAll()
                                .doOnComplete(() -> LOGGER.debug("Fetched all parking sessions from DB successfully"))
                                .doOnError(e -> LOGGER.error("Error fetching all parking sessions from DB: {}",
                                                e.getMessage()));
        }

        /**
         * Active-session check across both stores: Redis answers for sessions created
         * since the cache warmed, PostgreSQL for anything older. Asking PostgreSQL
         * alone would miss a session that has been written to Redis but whose event is
         * still in the queue, and the same vehicle would be admitted twice.
         */
        public Mono<Boolean> hasActiveSession(Integer vehicleId) {
                return redisStateService.hasActiveSession(vehicleId)
                                .flatMap(activeInRedis -> activeInRedis
                                                ? Mono.just(true)
                                                : sessionRepository.hasActiveSession(vehicleId))
                                .doOnError(e -> LOGGER.error("Error checking active session for vehicle {}: {}",
                                                vehicleId, e.getMessage()));
        }

        public Mono<ParkingSessionResponseDto> getSessionByIdWithDetails(Integer id) {
                LOGGER.debug("Fetching parking session by id with details: {}", id);
                return sessionRepository.findByIdWithDetails(id)
                                .doOnSuccess(dto -> LOGGER.debug("Fetched parking session with details: {}", dto))
                                .doOnError(e -> LOGGER.error("Error fetching parking session by id {} with details: {}",
                                                id,
                                                e.getMessage()));
        }

        // No @Transactional: Redis and RabbitMQ only; PostgreSQL is written by the
        // consumer, inside its own transaction.
        public Mono<Integer> updateSessionStatus(
                        Integer id,
                        String status,
                        Integer currentVersion) {

                LOGGER.info("Updating parking session status id: {} to status: {}", id, status);
                if (status == null || status.isBlank()) {
                        return Mono.error(
                                        new IllegalArgumentException("Session status cannot be empty."));
                }

                String normalizedStatus = status.trim().toUpperCase();

                // Optimistic lock is now evaluated against Redis, the real-time state, and
                // re-checked against session_version by the consumer when it persists.
                // The version travels on the event as `version`; the consumer derives the
                // expected row version as version-1, so exactly one transition can win.
                return getSessionById(id)
                                .switchIfEmpty(Mono.error(new IllegalStateException(
                                                "Concurrency conflict or session not found.")))
                                .flatMap(existing -> {
                                        int existingVersion = existing.sessionVersion() == null
                                                        ? (int) INITIAL_VERSION
                                                        : existing.sessionVersion();

                                        if (currentVersion != null && !currentVersion.equals(existingVersion)) {
                                                return Mono.<Integer>error(new IllegalStateException(
                                                                "Concurrency conflict: expected session_version "
                                                                                + currentVersion + " but real-time state is at "
                                                                                + existingVersion + "."));
                                        }

                                        long newVersion = existingVersion + 1L;

                                        return redisStateService
                                                        .updateSessionStatus(id, normalizedStatus, newVersion)
                                                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                                                        "Session " + id
                                                                                        + " is not present in the real-time store.")))
                                                        .flatMap(stored -> {
                                                                broadcastSessionUpdated(id, normalizedStatus);
                                                                return eventPublisher.publishSessionStatusChanged(
                                                                                stored, newVersion);
                                                        })
                                                        .thenReturn(1);
                                })
                                .doOnSuccess(rows -> LOGGER.info(
                                                "Parking session {} status set to {} in Redis and published, accepted: {}",
                                                id, normalizedStatus, rows))
                                .doOnError(e -> LOGGER.error("Error updating parking session status id {}: {}", id,
                                                e.getMessage()));
        }

        /** First version assigned to a session, in Redis and in session_version alike. */
        private static final long INITIAL_VERSION = 1L;

        private void broadcastSessionUpdated(Integer sessionId, String newStatus) {
                LOGGER.debug("Session {} updated to status {}", sessionId, newStatus);
        }

        private SessionEventPayload toSessionEventPayload(ParkingSessionDto dto) {
                return new SessionEventPayload(
                                dto.sessionId(),
                                dto.reservationId(),
                                dto.slotId(),
                                dto.userId(),
                                dto.vehicleId(),
                                dto.entryGateId(),
                                dto.exitGateId(),
                                dto.sessionStatus(),
                                dto.actualEntryTime(),
                                dto.actualExitTime(),
                                dto.expectedExitTime(),
                                dto.durationMinutes(),
                                dto.paymentStatus(),
                                dto.sessionVersion(),
                                LocalDateTime.now());
        }

        // Removed: toSessionEventPayload(sessionId, status). It built a payload whose
        // every other field was null and published it. The consumer wrote that payload
        // straight over the cached session, wiping slotId, userId and sessionVersion,
        // after which the next read returned a null version, the optimistic lock
        // compared against the wrong number, and the exit update matched zero rows.
        // Status changes now publish the payload Redis returned.

        private ParkingSessionDto toParkingSessionDto(SessionEventPayload payload) {
                return new ParkingSessionDto(
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
                                payload.updatedAt());
        }
}
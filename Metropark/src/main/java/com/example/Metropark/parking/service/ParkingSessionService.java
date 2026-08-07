package com.example.Metropark.parking.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.event.EventPublisher;
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

        @Transactional
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

                Mono<Void> slotValidation = slotRepository.findById(dto.slotId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("Parking slot not found.")))
                                .flatMap(slot -> {

                                        if (dto.reservationId() != null) {

                                                if ("RESERVED".equalsIgnoreCase(slot.currentStatus())) {
                                                        return Mono.empty();
                                                }

                                                return Mono.error(new IllegalStateException(
                                                                "Reserved session requires slot status RESERVED."));
                                        }

                                        if ("AVAILABLE".equalsIgnoreCase(slot.currentStatus())) {
                                                return Mono.empty();
                                        }

                                        return Mono.error(new IllegalStateException(
                                                        "Parking slot is " + slot.currentStatus()));
                                });

                Mono<Void> userValidation = userRepository.findById(dto.userId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("User not found.")))
                                .flatMap(user -> "ACTIVE".equalsIgnoreCase(user.userStatus())
                                                ? Mono.<Void>empty()
                                                : Mono.error(new IllegalStateException("User account is inactive.")));

                Mono<Void> checkDuplicateSession = sessionRepository.hasActiveSession(dto.vehicleId())
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

                                        return sessionRepository.create(session)
                                                        .flatMap(sessionId -> {
                                                                // Update the session with the generated ID
                                                                ParkingSessionDto savedSession = new ParkingSessionDto(
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
                                                                                session.sessionVersion(),
                                                                                session.createdAt(),
                                                                                session.updatedAt());

                                                                long version = 1;

                                                                // Save to Redis
                                                                return redisStateService
                                                                                .saveSession(savedSession, version)
                                                                                .then(redisStateService
                                                                                                .incrementVersion(
                                                                                                                "session",
                                                                                                                sessionId.toString()))
                                                                                .then(eventPublisher
                                                                                                .publishSessionStarted(
                                                                                                                toSessionEventPayload(
                                                                                                                                savedSession),
                                                                                                                version))
                                                                                .thenReturn(sessionId);
                                                        })
                                                        .doOnSuccess(sessionId -> LOGGER.info(
                                                                        "Parking session created successfully. Session ID: {}",
                                                                        sessionId))
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

        public Mono<Boolean> hasActiveSession(Integer vehicleId) {
                return sessionRepository.hasActiveSession(vehicleId)
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

        @Transactional
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

                return sessionRepository
                                .updateStatusWithOptimisticLock(
                                                id,
                                                normalizedStatus,
                                                currentVersion)
                                .flatMap(rowsUpdated -> {
                                        if (rowsUpdated > 0) {

                                                broadcastSessionUpdated(id, normalizedStatus);

                                                // Increment version and update Redis
                                                return redisStateService.incrementVersion("session", id.toString())
                                                                .flatMap(version -> redisStateService
                                                                                .updateSessionStatus(id,
                                                                                                normalizedStatus,
                                                                                                version)
                                                                                .then(eventPublisher
                                                                                                .publishSessionStatusChanged(
                                                                                                                toSessionEventPayload(
                                                                                                                                id,
                                                                                                                                normalizedStatus),
                                                                                                                version)))
                                                                .thenReturn(rowsUpdated);
                                        } else {
                                                return Mono.error(new IllegalStateException(
                                                                "Concurrency conflict or session not found."));
                                        }
                                })
                                .doOnSuccess(rows -> LOGGER.info(
                                                "Parking session status updated successfully, rows affected: {}", rows))
                                .doOnError(e -> LOGGER.error("Error updating parking session status id {}: {}", id,
                                                e.getMessage()));
        }

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

        private SessionEventPayload toSessionEventPayload(Integer sessionId, String status) {
                return new SessionEventPayload(
                                sessionId,
                                null, // reservationId
                                null, // slotId
                                null, // userId
                                null, // vehicleId
                                null, // entryGateId
                                null, // exitGateId
                                status,
                                null, // actualEntryTime
                                null, // actualExitTime
                                null, // expectedExitTime
                                null, // durationMinutes
                                null, // paymentStatus
                                null, // sessionVersion
                                LocalDateTime.now());
        }

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
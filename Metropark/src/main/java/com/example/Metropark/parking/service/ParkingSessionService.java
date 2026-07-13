package com.example.Metropark.parking.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
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

        public ParkingSessionService(
                        ParkingSessionRepository sessionRepository,
                        VehicleRepository vehicleRepository,
                        ParkingSlotRepository slotRepository,
                        ReservationRepository reservationRepository,
                        UserRepository userRepository) {

                this.sessionRepository = sessionRepository;
                this.vehicleRepository = vehicleRepository;
                this.slotRepository = slotRepository;
                this.reservationRepository = reservationRepository;
                this.userRepository = userRepository;
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
                                .flatMap(slot -> "AVAILABLE".equalsIgnoreCase(slot.currentStatus())
                                                ? Mono.<Void>empty()
                                                : Mono.error(new IllegalStateException(
                                                                "Parking slot is " + slot.currentStatus() + ".")));

                Mono<Void> reservationValidation = dto.reservationId() == null
                                ? Mono.empty()
                                : reservationRepository.findById(dto.reservationId())
                                                .switchIfEmpty(Mono.error(
                                                                new IllegalArgumentException("Reservation not found.")))
                                                .flatMap(reservation -> "RESERVED"
                                                                .equalsIgnoreCase(reservation.reservationStatus())
                                                                                ? Mono.<Void>empty()
                                                                                : Mono.error(new IllegalStateException(
                                                                                                "Reservation is "
                                                                                                                + reservation.reservationStatus()
                                                                                                                + ".")));

                Mono<Void> userValidation = userRepository.findById(dto.userId())
                                .switchIfEmpty(Mono.error(new IllegalArgumentException("User not found.")))
                                .flatMap(user -> "ACTIVE".equalsIgnoreCase(user.userStatus())
                                                ? Mono.<Void>empty()
                                                : Mono.error(new IllegalStateException("User account is inactive.")));
                // Check if the vehicle is already parked somewhere else!
                Mono<Void> checkDuplicateSession = sessionRepository.hasActiveSession(dto.vehicleId())
                                .flatMap(hasSession -> {
                                        if (hasSession) {
                                                return Mono.error(new IllegalStateException(
                                                                "Cannot start session: This vehicle is already parked or has a pending entry in another location."));
                                        }
                                        return Mono.<Void>empty();
                                });

                return Mono.when(
                                vehicleValidation,
                                slotValidation,
                                reservationValidation,
                                userValidation,
                                checkDuplicateSession)
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
                                                        .doOnSuccess(rows -> LOGGER.info(
                                                                        "Parking session created successfully, rows affected: {}",
                                                                        rows))
                                                        .doOnError(e -> LOGGER.error(
                                                                        "Error creating parking session: {}",
                                                                        e.getMessage()));
                                }));
        }

        public Flux<ParkingSessionDto> getAllSessions() {
                LOGGER.debug("Fetching all parking sessions");
                return sessionRepository.findAll()
                                .doOnComplete(() -> LOGGER.debug("Fetched all parking sessions successfully"))
                                .doOnError(e -> LOGGER.error("Error fetching all parking sessions: {}",
                                                e.getMessage()));
        }

        public Mono<ParkingSessionDto> getSessionById(Integer id) {
                LOGGER.debug("Fetching parking session by id: {}", id);
                return sessionRepository.findById(id)
                                .doOnSuccess(dto -> LOGGER.debug("Fetched parking session: {}", dto))
                                .doOnError(e -> LOGGER.error("Error fetching parking session by id {}: {}", id,
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

                return sessionRepository
                                .updateStatusWithOptimisticLock(
                                                id,
                                                status.trim().toUpperCase(),
                                                currentVersion)
                                .flatMap(rowsUpdated -> rowsUpdated > 0
                                                ? Mono.just(rowsUpdated)
                                                : Mono.error(new IllegalStateException(
                                                                "Concurrency conflict or session not found.")))
                                .doOnSuccess(rows -> LOGGER.info(
                                                "Parking session status updated successfully, rows affected: {}", rows))
                                .doOnError(e -> LOGGER.error("Error updating parking session status id {}: {}", id,
                                                e.getMessage()));
        }
}
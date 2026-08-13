package com.example.Metropark.reservation.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.event.payload.ReservationEventPayload;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.reservation.dto.ReservationDto;
import com.example.Metropark.reservation.repo.ReservationRepository;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReservationService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ReservationService.class);

        private final ReservationRepository reservationRepository;
        private final ParkingSlotRepository parkingSlotRepository;
        private final RedisStateService redisStateService;

        public ReservationService(ReservationRepository reservationRepository,
                        ParkingSlotRepository parkingSlotRepository,
                        RedisStateService redisStateService) {
                this.reservationRepository = reservationRepository;
                this.parkingSlotRepository = parkingSlotRepository;
                this.redisStateService = redisStateService;
        }

        @Transactional
        public Mono<Integer> createReservation(ReservationDto dto) {

                LOGGER.info("Creating reservation: {}", dto);

                if (dto.userId() == null || dto.slotId() == null) {
                        return Mono.error(new IllegalArgumentException(
                                        "User ID and Slot ID are strictly required."));
                }

                LocalDateTime now = LocalDateTime.now();

                LocalDateTime expiry = dto.expiresAt() != null
                                ? dto.expiresAt()
                                : now.plusMinutes(30);

                String reservationStatus = dto.queueEntryId() != null
                                ? "WAITING"
                                : "RESERVED";

                ReservationDto reservation = new ReservationDto(
                                null, // Database generates the ID
                                dto.userId(),
                                dto.slotId(),
                                dto.queueEntryId(),
                                reservationStatus,
                                1,
                                now,
                                expiry,
                                now,
                                now);

                return parkingSlotRepository.reserveSlot(dto.slotId())
                                .flatMap(rowsUpdated -> {

                                        if (rowsUpdated == 0) {
                                                return Mono.error(new IllegalStateException(
                                                                "Slot is already reserved or does not exist."));
                                        }

                                        return reservationRepository.create(reservation)
                                                        .flatMap(reservationId -> {
                                                                ReservationDto savedReservation = new ReservationDto(
                                                                                reservationId,
                                                                                reservation.userId(),
                                                                                reservation.slotId(),
                                                                                reservation.queueEntryId(),
                                                                                reservation.reservationStatus(),
                                                                                reservation.reservationVersion(),
                                                                                reservation.reservedAt(),
                                                                                reservation.expiresAt(),
                                                                                reservation.createdAt(),
                                                                                reservation.updatedAt());

                                                                long version = 1;

                                                                return redisStateService
                                                                                .saveReservation(savedReservation,
                                                                                                version)
                                                                                .then(redisStateService
                                                                                                .incrementVersion(
                                                                                                                "reservation",
                                                                                                                reservationId.toString()))
                                                                                .thenReturn(rowsUpdated);
                                                        })
                                                        .doOnSuccess(id -> LOGGER.info(
                                                                        "Reservation created successfully. Reservation ID: {}",
                                                                        id))
                                                        .doOnError(e -> LOGGER.error("Error creating reservation", e));
                                });
        }

        public Flux<ReservationDto> getAllReservations() {
                LOGGER.debug("Fetching all reservations");
                return redisStateService.getAllReservations()
                                .map(this::toReservationDto)
                                .switchIfEmpty(reservationRepository.findAll())
                                .doOnComplete(() -> LOGGER.debug("Fetched all reservations successfully"))
                                .doOnError(e -> LOGGER.error("Error fetching all reservations: {}", e.getMessage()));
        }

        public Mono<ReservationDto> getReservationById(Integer id) {
                LOGGER.debug("Fetching reservation by id: {}", id);
                return redisStateService.getReservation(id)
                                .map(this::toReservationDto)
                                .switchIfEmpty(reservationRepository.findById(id))
                                .doOnSuccess(dto -> LOGGER.debug("Fetched reservation: {}", dto))
                                .doOnError(e -> LOGGER.error("Error fetching reservation by id {}: {}", id,
                                                e.getMessage()));
        }

        @Transactional
        public Mono<Integer> updateStatus(Integer id, String status, Integer currentVersion) {
                LOGGER.info("Updating reservation status id: {} to status: {}", id, status);
                if (status == null || status.isBlank()) {
                        return Mono.error(new IllegalArgumentException("Status cannot be empty."));
                }

                String normalizedStatus = status.trim().toUpperCase();

                return reservationRepository.updateStatusWithOptimisticLock(id, normalizedStatus, currentVersion)
                                .flatMap(rowsUpdated -> {
                                        if (rowsUpdated == 0) {
                                                return Mono.error(new IllegalStateException(
                                                                "Update failed: Concurrency conflict or Reservation not found. Please refresh and try again."));
                                        }

                                        return redisStateService.incrementVersion("reservation", id.toString())
                                                        .flatMap(version -> redisStateService.updateReservationStatus(
                                                                        id, normalizedStatus, version))
                                                        .thenReturn(rowsUpdated);
                                })
                                .doOnSuccess(rows -> LOGGER.info(
                                                "Reservation status updated successfully, rows affected: {}", rows))
                                .doOnError(e -> LOGGER.error("Error updating reservation status id {}: {}", id,
                                                e.getMessage()));
        }

        private ReservationEventPayload toReservationEventPayload(ReservationDto dto) {
                return new ReservationEventPayload(
                                dto.reservationId(),
                                dto.userId(),
                                dto.slotId(),
                                dto.queueEntryId(),
                                dto.reservationStatus(),
                                dto.reservationVersion(),
                                dto.reservedAt(),
                                dto.expiresAt(),
                                LocalDateTime.now());
        }

        private ReservationEventPayload toReservationEventPayload(Integer reservationId, String status) {
                return new ReservationEventPayload(
                                reservationId,
                                null, // userId
                                null, // slotId
                                null, // queueEntryId
                                status,
                                null, // reservationVersion
                                null, // reservedAt
                                null, // expiresAt
                                LocalDateTime.now());
        }

        private ReservationDto toReservationDto(ReservationEventPayload payload) {
                return new ReservationDto(
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

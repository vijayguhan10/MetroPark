package com.example.Metropark.reservation.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.reservation.dto.ReservationDto;
import com.example.Metropark.reservation.repo.ReservationRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReservationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReservationService.class);

    private final ReservationRepository reservationRepository;
    private final ParkingSlotRepository parkingSlotRepository;

    public ReservationService(ReservationRepository reservationRepository,
            ParkingSlotRepository parkingSlotRepository) {
        this.reservationRepository = reservationRepository;
        this.parkingSlotRepository = parkingSlotRepository;
    }

    @Transactional
    public Mono<Integer> createReservation(ReservationDto dto) {

        LOGGER.info("Creating reservation: {}", dto);
        if (dto.userId() == null || dto.slotId() == null) {
            return Mono.error(
                    new IllegalArgumentException(
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
                dto.reservationId(),
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

                        return Mono.error(

                                new IllegalStateException(
                                        "Slot is already reserved or does not exist.")

                        );
                    }

                    return reservationRepository.create(reservation)
                            .doOnSuccess(rows -> LOGGER.info("Reservation created successfully, rows affected: {}", rows))
                            .doOnError(e -> LOGGER.error("Error creating reservation: {}", e.getMessage()));

                });
    }

    public Flux<ReservationDto> getAllReservations() {
        LOGGER.debug("Fetching all reservations");
        return reservationRepository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all reservations successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all reservations: {}", e.getMessage()));
    }

    public Mono<ReservationDto> getReservationById(Integer id) {
        LOGGER.debug("Fetching reservation by id: {}", id);
        return reservationRepository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched reservation: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching reservation by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateStatus(Integer id, String status, Integer currentVersion) {
        LOGGER.info("Updating reservation status id: {} to status: {}", id, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return reservationRepository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException(
                                "Update failed: Concurrency conflict or Reservation not found. Please refresh and try again."));
                    }
                    LOGGER.info("Reservation status updated successfully, rows affected: {}", rowsUpdated);
                    return Mono.just(rowsUpdated);
                })
                .doOnError(e -> LOGGER.error("Error updating reservation status id {}: {}", id, e.getMessage()));
    }
}
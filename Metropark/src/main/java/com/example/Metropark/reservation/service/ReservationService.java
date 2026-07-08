package com.example.Metropark.reservation.service;

import com.example.Metropark.reservation.dto.ReservationDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.reservation.repo.ReservationRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ParkingSlotRepository parkingSlotRepository;

    public ReservationService(ReservationRepository reservationRepository,
            ParkingSlotRepository parkingSlotRepository) {
        this.reservationRepository = reservationRepository;
        this.parkingSlotRepository = parkingSlotRepository;
    }

    @Transactional
    public Mono<Integer> createReservation(ReservationDto dto) {

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

                    return reservationRepository.create(reservation);

                });
    }

    public Flux<ReservationDto> getAllReservations() {
        return reservationRepository.findAll();
    }

    public Mono<ReservationDto> getReservationById(Integer id) {
        return reservationRepository.findById(id);
    }

    public Mono<Integer> updateStatus(Integer id, String status, Integer currentVersion) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return reservationRepository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException(
                                "Update failed: Concurrency conflict or Reservation not found. Please refresh and try again."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
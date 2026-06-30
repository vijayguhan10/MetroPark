package com.example.Metropark.service;

import com.example.Metropark.dto.ReservationDto;
import com.example.Metropark.repo.ReservationRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class ReservationService {

    private final ReservationRepository repository;

    public ReservationService(ReservationRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createReservation(ReservationDto dto) {
        if (dto.userId() == null || dto.slotId() == null) {
            return Mono.error(new IllegalArgumentException("User ID and Slot ID are strictly required."));
        }

        LocalDateTime now = LocalDateTime.now();
        
        LocalDateTime expiry = (dto.expiresAt() != null) ? dto.expiresAt() : now.plusMinutes(30);
        
        String status = (dto.queueEntryId() != null) ? "WAITING" : "RESERVED";

        ReservationDto processedDto = new ReservationDto(
                dto.reservationId(),
                dto.userId(),
                dto.slotId(),
                dto.queueEntryId(),
                status,
                1, 
                now, // Reserved at
                expiry,
                now, // Created at
                now  // Updated at
        );

        return repository.create(processedDto);
    }

    public Flux<ReservationDto> getAllReservations() {
        return repository.findAll();
    }

    public Mono<ReservationDto> getReservationById(Integer id) {
        return repository.findById(id);
    }

    // Handles the Concurrency Check
    public Mono<Integer> updateStatus(Integer id, String status, Integer currentVersion) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }
        
        return repository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        // If 0 rows updated, it means either the ID doesn't exist OR the version was wrong!
                        return Mono.error(new IllegalStateException("Update failed: Concurrency conflict or Reservation not found. Please refresh and try again."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
package com.example.Metropark.service;

import com.example.Metropark.dto.ParkingSessionDto;
import com.example.Metropark.repo.ParkingSessionRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class ParkingSessionService {

    private final ParkingSessionRepository repository;

    public ParkingSessionService(ParkingSessionRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createSession(ParkingSessionDto dto) {
        if (dto.slotId() == null || dto.userId() == null || dto.vehicleId() == null) {
            return Mono.error(new IllegalArgumentException("Slot ID, User ID, and Vehicle ID are required."));
        }

        String status = (dto.sessionStatus() == null || dto.sessionStatus().isBlank()) 
                ? "CREATED" 
                : dto.sessionStatus().trim().toUpperCase();
                
        String payment = (dto.paymentStatus() == null || dto.paymentStatus().isBlank()) 
                ? "PENDING" 
                : dto.paymentStatus().trim().toUpperCase();

        LocalDateTime now = LocalDateTime.now();

        ParkingSessionDto cleanDto = new ParkingSessionDto(
                dto.sessionId(),
                dto.reservationId(),
                dto.slotId(),
                dto.userId(),
                dto.vehicleId(),
                dto.entryGateId(),
                dto.exitGateId(),
                status,
                dto.actualEntryTime(),
                dto.actualExitTime(),
                dto.expectedExitTime(),
                dto.durationMinutes(),
                payment,
                1, 
                now,
                now
        );

        return repository.create(cleanDto);
    }

    public Flux<ParkingSessionDto> getAllSessions() {
        return repository.findAll();
    }

    public Mono<ParkingSessionDto> getSessionById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateSessionStatus(Integer id, String status, Integer currentVersion) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Session status cannot be empty."));
        }

        return repository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Update failed: Concurrency conflict or Session not found."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
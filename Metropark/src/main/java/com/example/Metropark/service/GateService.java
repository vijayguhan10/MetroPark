package com.example.Metropark.service;

import com.example.Metropark.dto.GateDto;
import com.example.Metropark.repo.GateRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class GateService {

    private final GateRepository repository;

    public GateService(GateRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createGate(GateDto dto) {
        if (dto.locationId() == null || dto.locationId().isBlank()) {
            return Mono.error(new IllegalArgumentException("Location ID is required."));
        }
        if (dto.gateName() == null || dto.gateName().isBlank()) {
            return Mono.error(new IllegalArgumentException("Gate Name is required."));
        }
        if (dto.gateType() == null || dto.gateType().isBlank()) {
            return Mono.error(new IllegalArgumentException("Gate Type (ENTRY, EXIT, BOTH) is required."));
        }

        String status = (dto.status() == null || dto.status().isBlank()) 
                ? "ACTIVE" 
                : dto.status().trim().toUpperCase();
                
        String cleanGateType = dto.gateType().trim().toUpperCase();
        LocalDateTime now = LocalDateTime.now();

        GateDto cleanDto = new GateDto(
                dto.gateId(),
                dto.locationId(),
                dto.gateName().trim(),
                cleanGateType,
                status,
                now,
                now
        );

        return repository.create(cleanDto);
    }

    public Flux<GateDto> getAllGates() {
        return repository.findAll();
    }

    public Mono<GateDto> getGateById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateGateStatus(Integer id, String status) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return repository.updateStatus(id, status.trim().toUpperCase())
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Gate not found."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
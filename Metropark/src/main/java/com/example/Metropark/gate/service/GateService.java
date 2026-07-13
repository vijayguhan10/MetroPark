 
 package com.example.Metropark.gate.service;

import java.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.gate.repo.GateRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class GateService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GateService.class);

    private final GateRepository repository;

    public GateService(GateRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createGate(GateDto dto) {
        LOGGER.info("Creating gate: {}", dto);
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

        return repository.create(cleanDto)
                .doOnSuccess(rows -> LOGGER.info("Gate created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating gate: {}", e.getMessage()));
    }

    public Flux<GateDto> getAllGates() {
        LOGGER.debug("Fetching all gates");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all gates successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all gates: {}", e.getMessage()));
    }

    public Mono<GateDto> getGateById(Integer id) {
        LOGGER.debug("Fetching gate by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched gate: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching gate by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateGateStatus(Integer id, String status) {
        LOGGER.info("Updating gate status id: {} to status: {}", id, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return repository.updateStatus(id, status.trim().toUpperCase())
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Gate not found."));
                    }
                    LOGGER.info("Gate status updated successfully, rows affected: {}", rowsUpdated);
                    return Mono.just(rowsUpdated);
                })
                .doOnError(e -> LOGGER.error("Error updating gate status id {}: {}", id, e.getMessage()));
    }
}
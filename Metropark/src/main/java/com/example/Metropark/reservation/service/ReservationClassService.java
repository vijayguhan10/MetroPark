package com.example.Metropark.reservation.service;

import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.repo.ReservationClassRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReservationClassService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReservationClassService.class);

    private final ReservationClassRepository repository;

    public ReservationClassService(ReservationClassRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createReservationClass(ReservationClassDto dto) {
        LOGGER.info("Creating reservation class: {}", dto);
        if (dto.className() == null || dto.className().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Class name cannot be empty."));
        }

        String formattedName = convertToPascalCase(dto.className());
        return repository.create(new ReservationClassDto(dto.classId(), formattedName))
                .doOnSuccess(rows -> LOGGER.info("Reservation class created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating reservation class: {}", e.getMessage()));
    }

    public Flux<ReservationClassDto> getAllReservationClasses() {
        LOGGER.debug("Fetching all reservation classes");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all reservation classes successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all reservation classes: {}", e.getMessage()));
    }

    public Mono<ReservationClassDto> getReservationClassById(Integer id) {
        LOGGER.debug("Fetching reservation class by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched reservation class: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching reservation class by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateReservationClass(Integer id, ReservationClassDto dto) {
        LOGGER.info("Updating reservation class id: {} with data: {}", id, dto);
        if (dto.className() == null || dto.className().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Class name cannot be empty."));
        }

        String formattedName = convertToPascalCase(dto.className());
        return repository.update(id, new ReservationClassDto(dto.classId(), formattedName))
                .doOnSuccess(rows -> LOGGER.info("Reservation class updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating reservation class id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> deleteReservationClass(Integer id) {
        LOGGER.info("Deleting reservation class id: {}", id);
        return repository.delete(id)
                .doOnSuccess(rows -> LOGGER.info("Reservation class deleted successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error deleting reservation class id {}: {}", id, e.getMessage()));
    }

    // Robust Helper: Standardizes inputs like "vip" -> "Vip" or "GENERAL" -> "General"
    private String convertToPascalCase(String input) {
        String trimmed = input.trim();
        if (trimmed.isEmpty()) return trimmed;
        return trimmed.substring(0, 1).toUpperCase() + trimmed.substring(1).toLowerCase();
    }
}
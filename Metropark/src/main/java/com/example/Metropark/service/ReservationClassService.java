package com.example.Metropark.service;

import com.example.Metropark.dto.ReservationClassDto;
import com.example.Metropark.repo.ReservationClassRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ReservationClassService {

    private final ReservationClassRepository repository;

    public ReservationClassService(ReservationClassRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createReservationClass(ReservationClassDto dto) {
        if (dto.className() == null || dto.className().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Class name cannot be empty."));
        }

        String formattedName = convertToPascalCase(dto.className());
        return repository.create(new ReservationClassDto(dto.classId(), formattedName));
    }

    public Flux<ReservationClassDto> getAllReservationClasses() {
        return repository.findAll();
    }

    public Mono<ReservationClassDto> getReservationClassById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateReservationClass(Integer id, ReservationClassDto dto) {
        if (dto.className() == null || dto.className().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Class name cannot be empty."));
        }

        String formattedName = convertToPascalCase(dto.className());
        return repository.update(id, new ReservationClassDto(dto.classId(), formattedName));
    }

    public Mono<Integer> deleteReservationClass(Integer id) {
        return repository.delete(id);
    }

    // Robust Helper: Standardizes inputs like "vip" -> "Vip" or "GENERAL" -> "General"
    private String convertToPascalCase(String input) {
        String trimmed = input.trim();
        if (trimmed.isEmpty()) return trimmed;
        return trimmed.substring(0, 1).toUpperCase() + trimmed.substring(1).toLowerCase();
    }
}
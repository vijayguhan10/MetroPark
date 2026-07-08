package com.example.Metropark.queue.service;

import com.example.Metropark.queue.dto.QueueDto;
import com.example.Metropark.queue.repo.QueueRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class QueueService {

    private final QueueRepository repository;

    public QueueService(QueueRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createQueue(QueueDto dto) {
        if (dto.locationId() == null || dto.queueName() == null || dto.queueName().isBlank()) {
            return Mono.error(new IllegalArgumentException("Location ID and Queue Name are strictly required."));
        }
        if (dto.vehicleTypeId() == null || dto.reservationClassId() == null) {
            return Mono.error(new IllegalArgumentException("Vehicle Type ID and Reservation Class ID are required to categorize the queue."));
        }

        String status = (dto.status() == null || dto.status().isBlank()) 
                ? "ACTIVE" 
                : dto.status().trim().toUpperCase();

        QueueDto cleanDto = new QueueDto(
                dto.queueId(),
                dto.locationId(),
                dto.vehicleTypeId(),
                dto.reservationClassId(),
                dto.queueName().trim(),
                status,
                LocalDateTime.now()
        );

        return repository.create(cleanDto);
    }

    public Flux<QueueDto> getAllQueues() {
        return repository.findAll();
    }

    public Mono<QueueDto> getQueueById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateQueueStatus(Integer id, String status) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return repository.updateStatus(id, status.trim().toUpperCase())
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Update failed: Queue not found."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
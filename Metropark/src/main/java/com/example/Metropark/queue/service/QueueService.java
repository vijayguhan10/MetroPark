package com.example.Metropark.queue.service;

import com.example.Metropark.queue.dto.QueueDto;
import com.example.Metropark.queue.repo.QueueRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class QueueService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueueService.class);

    private final QueueRepository repository;

    public QueueService(QueueRepository repository) {
        this.repository = repository;
    }

    @Transactional
    public Mono<Integer> createQueue(QueueDto dto) {
        LOGGER.info("Creating queue: {}", dto);
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

        return repository.create(cleanDto)
                .doOnSuccess(rows -> LOGGER.info("Queue created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating queue: {}", e.getMessage()));
    }

    public Flux<QueueDto> getAllQueues() {
        LOGGER.debug("Fetching all queues");
        return repository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all queues successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all queues: {}", e.getMessage()));
    }

    public Mono<QueueDto> getQueueById(Integer id) {
        LOGGER.debug("Fetching queue by id: {}", id);
        return repository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched queue: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching queue by id {}: {}", id, e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateQueueStatus(Integer id, String status) {
        LOGGER.info("Updating queue status id: {} to status: {}", id, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return repository.updateStatus(id, status.trim().toUpperCase())
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(new IllegalStateException("Update failed: Queue not found."));
                    }
                    LOGGER.info("Queue status updated successfully, rows affected: {}", rowsUpdated);
                    return Mono.just(rowsUpdated);
                })
                .doOnError(e -> LOGGER.error("Error updating queue status id {}: {}", id, e.getMessage()));
    }
}
package com.example.Metropark.service;

import org.springframework.stereotype.Service;

import com.example.Metropark.dto.ParkingSlotDto;
import com.example.Metropark.repo.ParkingSlotRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;


@Service
public class ParkingSlotService {

    private final ParkingSlotRepository repository;

    public ParkingSlotService(ParkingSlotRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createSlot(ParkingSlotDto dto) {
        if (dto.locationId() == null || dto.displayCode() == null || dto.sensorId() == null) {
            return Mono.error(new IllegalArgumentException("Location ID, Display Code, and Sensor ID are required."));
        }

        String status = (dto.currentStatus() == null || dto.currentStatus().isBlank())
                ? "AVAILABLE"
                : dto.currentStatus().trim().toUpperCase();

        ParkingSlotDto cleanDto = new ParkingSlotDto(
                dto.slotId(),
                dto.locationId(),
                dto.displayCode().trim().toUpperCase(),
                dto.vehicleTypeId(),
                dto.reservationClassId(),
                dto.sensorId().trim(),
                status);

        return repository.create(cleanDto);
    }

    public Flux<ParkingSlotDto> getAllSlots() {
        return repository.findAll();
    }

    public Mono<ParkingSlotDto> getSlotById(Integer id) {
        return repository.findById(id);
    }

    // public Mono<Integer> updateSlotStatus(Integer id, String status, Integer
    // currentVersion) {
    // if (status == null || status.isBlank()) {
    // return Mono.error(new IllegalArgumentException("Status cannot be empty."));
    // }

    // return repository.updateStatusWithOptimisticLock(id,
    // status.trim().toUpperCase(), currentVersion)
    // .flatMap(rowsUpdated -> {
    // if (rowsUpdated == 0) {
    // return Mono.error(new IllegalStateException("Update failed: Someone else
    // modified this slot just before you did, or the slot does not exist. Please
    // refresh and try again."));
    // }
    // return Mono.just(rowsUpdated);
    // });
    // }
}
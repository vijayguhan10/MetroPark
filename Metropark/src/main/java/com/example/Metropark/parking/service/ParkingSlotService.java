package com.example.Metropark.parking.service;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.event.payload.ReservationEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.dto.SlotAvailabilityRequestDto;
import com.example.Metropark.parking.dto.SlotAvailabilityResponseDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class ParkingSlotService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParkingSlotService.class);

        private final ParkingSlotRepository repository;
        private final RedisStateService redisStateService;

        private static final Set<String> OCCUPIED_SLOT_STATUSES = Set.of("OCCUPIED", "RESERVED", "MAINTENANCE");
        
        private static final Set<String> ACTIVE_SESSION_STATUSES = Set.of("RESERVED", "CREATED", "ACTIVE");
        
        private static final Set<String> ACTIVE_RESERVATION_STATUSES = Set.of("RESERVED", "WAITING");

        public ParkingSlotService(
                        ParkingSlotRepository repository,
                        RedisStateService redisStateService) {

                this.repository = repository;
                this.redisStateService = redisStateService;
        }

        public Mono<Integer> createSlot(ParkingSlotDto dto) {

                LOGGER.info("Creating parking slot: {}", dto);

                if (dto.locationId() == null
                                || dto.displayCode() == null
                                || dto.sensorId() == null) {

                        return Mono.error(
                                        new IllegalArgumentException(
                                                        "Location ID, Display Code, and Sensor ID are required."));
                }

                String status = dto.currentStatus() == null
                                || dto.currentStatus().isBlank()
                                                ? "AVAILABLE"
                                                : dto.currentStatus()
                                                                .trim()
                                                                .toUpperCase();

                ParkingSlotDto cleanDto = new ParkingSlotDto(
                                dto.slotId(),
                                dto.locationId(),
                                dto.displayCode().trim().toUpperCase(),
                                dto.vehicleTypeId(),
                                dto.reservationClassId(),
                                dto.sensorId().trim(),
                                status);

                return repository.create(cleanDto)
                                .flatMap(slotId -> {

                                        ParkingSlotDto savedDto = new ParkingSlotDto(
                                                        slotId,
                                                        cleanDto.locationId(),
                                                        cleanDto.displayCode(),
                                                        cleanDto.vehicleTypeId(),
                                                        cleanDto.reservationClassId(),
                                                        cleanDto.sensorId(),
                                                        cleanDto.currentStatus());

                                        long version = 1L;

                                        return redisStateService
                                                        .saveSlotState(
                                                                        toSlotEventPayload(savedDto),
                                                                        version)
                                                        .thenReturn(slotId);
                                })
                                .doOnSuccess(slotId -> LOGGER.info(
                                                "Parking slot created successfully, slot id: {}",
                                                slotId))
                                .doOnError(e -> LOGGER.error(
                                                "Error creating parking slot: {}",
                                                e.getMessage()));
        }

        public Mono<Integer> createSlots(
                        List<ParkingSlotDto> dtos) {

                if (dtos == null || dtos.isEmpty()) {
                        return Mono.error(
                                        new IllegalArgumentException(
                                                        "Parking slots list cannot be empty."));
                }

                LOGGER.info(
                                "Creating {} parking slots",
                                dtos.size());

                for (ParkingSlotDto dto : dtos) {

                        if (dto.locationId() == null
                                        || dto.displayCode() == null
                                        || dto.sensorId() == null) {

                                return Mono.error(
                                                new IllegalArgumentException(
                                                                "Location ID, Display Code, and Sensor ID are required for all slots."));
                        }
                }

                return Flux.fromIterable(dtos)
                                .flatMap(this::createSlot)
                                .count()
                                .map(Long::intValue)
                                .doOnSuccess(total -> LOGGER.info(
                                                "Total parking slots created: {}",
                                                total))
                                .doOnError(e -> LOGGER.error(
                                                "Error creating parking slots: {}",
                                                e.getMessage()));
        }

        public Flux<ParkingSlotDto> getAllSlots() {

                LOGGER.debug("Fetching all parking slots");

                return redisStateService
                                .getAllSlots()
                                .map(this::toParkingSlotDto)
                                .switchIfEmpty(repository.findAll())
                                .doOnComplete(() -> LOGGER.debug(
                                                "Fetched all parking slots successfully"))
                                .doOnError(e -> LOGGER.error(
                                                "Error fetching all parking slots: {}",
                                                e.getMessage()));
        }

        public Mono<ParkingSlotDto> getSlotById(Integer id) {

                LOGGER.debug(
                                "Fetching parking slot by id: {}",
                                id);

                return redisStateService
                                .getSlot(id)
                                .map(this::toParkingSlotDto)
                                .switchIfEmpty(repository.findById(id))
                                .doOnSuccess(dto -> LOGGER.debug(
                                                "Fetched parking slot: {}",
                                                dto))
                                .doOnError(e -> LOGGER.error(
                                                "Error fetching parking slot by id {}: {}",
                                                id,
                                                e.getMessage()));
        }

        public Mono<Boolean> isSlotAvailable(Integer slotId) {
                return redisStateService
                                .getSlotStatus(slotId)
                                .switchIfEmpty(
                                        repository.findById(slotId)
                                                .map(ParkingSlotDto::currentStatus))
                                .map(status -> ParkingLifecycleService.SLOT_AVAILABLE
                                                .equalsIgnoreCase(status))
                                .doOnSuccess(available -> LOGGER.debug(
                                                "Slot {} availability check: {}",
                                                slotId, available))
                                .doOnError(e -> LOGGER.error(
                                                "Error checking availability for slot {}: {}",
                                                slotId, e.getMessage()));
        }

        public Mono<Integer> updateSlotStatus(
                        Integer id,
                        String status) {

                LOGGER.info(
                                "Updating parking slot status id: {} to status: {}",
                                id,
                                status);

                if (status == null || status.isBlank()) {
                        return Mono.error(
                                        new IllegalArgumentException(
                                                        "Status cannot be empty."));
                }

                String normalizedStatus = status.trim().toUpperCase();

                return currentSlotPayload(id)
                                .switchIfEmpty(
                                                Mono.error(
                                                                new IllegalStateException(
                                                                                "Update failed: Slot not found.")))
                                .flatMap(existing -> redisStateService
                                                .incrementVersion(
                                                                "slot",
                                                                id.toString())
                                                .flatMap(version -> redisStateService
                                                                .saveSlotState(
                                                                                new SlotEventPayload(
                                                                                                existing.slotId(),
                                                                                                existing.locationId(),
                                                                                                existing.displayCode(),
                                                                                                existing.vehicleTypeId(),
                                                                                                existing.reservationClassId(),
                                                                                                existing.sensorId(),
                                                                                                normalizedStatus,
                                                                                                LocalDateTime.now()),
                                                                                version)
                                                                .thenReturn(1)))
                                .doOnSuccess(rows -> LOGGER.info(
                                                "Parking slot {} set to {} in Redis and published for persistence",
                                                id,
                                                normalizedStatus))
                                .doOnError(e -> LOGGER.error(
                                                "Error updating parking slot status id {}: {}",
                                                id,
                                                e.getMessage()));
        }

        public Flux<Integer> getFilledSlotIds(
                        String locationId,
                        LocalDate date,
                        LocalTime fromTime,
                        LocalTime toTime) {

                LOGGER.info(
                                "Checking filled slots for location: {}, date: {}, from: {}, to: {}",
                                locationId, date, fromTime, toTime);

                if (locationId == null || locationId.isBlank()) {
                        return Flux.error(
                                        new IllegalArgumentException("Location ID is required."));
                }
                if (date == null) {
                        return Flux.error(
                                        new IllegalArgumentException("Date is required."));
                }
                if (fromTime == null || toTime == null) {
                        return Flux.error(
                                        new IllegalArgumentException("From time and to time are required."));
                }
                if (fromTime.isAfter(toTime) || fromTime.equals(toTime)) {
                        return Flux.error(
                                        new IllegalArgumentException("From time must be before to time."));
                }

                LocalDateTime rangeStart = date.atTime(fromTime);
                LocalDateTime rangeEnd = date.atTime(toTime);

                return redisStateService.getAllSlots()
                                .filter(slot -> locationId.equals(slot.locationId()))
                                .collectList()
                                .flatMapMany(slots -> {
                                        if (slots.isEmpty()) {
                                                LOGGER.debug("No slots found for location: {}", locationId);
                                                return Flux.empty();
                                        }

                                        return Mono.zip(
                                                        redisStateService.getAllReservations().collectList(),
                                                        redisStateService.getAllSessions().collectList())
                                                .flatMapMany(tuple -> {
                                                        List<ReservationEventPayload> reservations = tuple.getT1();
                                                        List<SessionEventPayload> sessions = tuple.getT2();

                                                        Set<Integer> locationSlotIds = slots.stream()
                                                                        .map(SlotEventPayload::slotId)
                                                                        .collect(Collectors.toSet());

                                                        List<ReservationEventPayload> relevantReservations = reservations.stream()
                                                                        .filter(r -> locationSlotIds.contains(r.slotId()))
                                                                        .filter(r -> ACTIVE_RESERVATION_STATUSES.contains(
                                                                                        r.reservationStatus() != null ? r.reservationStatus().toUpperCase() : ""))
                                                                        .toList();

                                                        List<SessionEventPayload> relevantSessions = sessions.stream()
                                                                        .filter(s -> locationSlotIds.contains(s.slotId()))
                                                                        .filter(s -> ACTIVE_SESSION_STATUSES.contains(
                                                                                        s.sessionStatus() != null ? s.sessionStatus().toUpperCase() : ""))
                                                                        .toList();

                                                        return Flux.fromIterable(slots)
                                                                        .filter(slot -> isSlotFilled(
                                                                                        slot,
                                                                                        relevantReservations,
                                                                                        relevantSessions,
                                                                                        rangeStart,
                                                                                        rangeEnd))
                                                                        .map(SlotEventPayload::slotId);
                                                });
                                })
                                .doOnComplete(() -> LOGGER.debug(
                                                "Completed filled slots check for location: {}", locationId))
                                .doOnError(e -> LOGGER.error(
                                                "Error checking filled slots for location {}: {}",
                                                locationId, e.getMessage()));
        }

        private boolean isSlotFilled(
                        SlotEventPayload slot,
                        List<ReservationEventPayload> reservations,
                        List<SessionEventPayload> sessions,
                        LocalDateTime rangeStart,
                        LocalDateTime rangeEnd) {

                String currentStatus = slot.currentStatus() != null ? slot.currentStatus().toUpperCase() : "";
                if (OCCUPIED_SLOT_STATUSES.contains(currentStatus)) {
                        return true;
                }

                for (ReservationEventPayload reservation : reservations) {
                        if (reservation.slotId().equals(slot.slotId())) {
                                if (reservation.reservedAt() != null && reservation.expiresAt() != null) {
                                        if (timeRangesOverlap(
                                                        reservation.reservedAt(),
                                                        reservation.expiresAt(),
                                                        rangeStart,
                                                        rangeEnd)) {
                                                return true;
                                        }
                                }
                        }
                }

                for (SessionEventPayload session : sessions) {
                        if (session.slotId().equals(slot.slotId())) {
                                LocalDateTime sessionStart = session.actualEntryTime() != null
                                                ? session.actualEntryTime()
                                                : session.expectedExitTime() != null
                                                                ? session.expectedExitTime().minusMinutes(
                                                                                session.durationMinutes() != null ? session.durationMinutes() : 60)
                                                                : rangeStart;
                                LocalDateTime sessionEnd = session.actualExitTime() != null
                                                ? session.actualExitTime()
                                                : session.expectedExitTime() != null
                                                                ? session.expectedExitTime()
                                                                : rangeEnd;

                                if (timeRangesOverlap(sessionStart, sessionEnd, rangeStart, rangeEnd)) {
                                        return true;
                                }
                        }
                }

                return false;
        }

        private boolean timeRangesOverlap(
                        LocalDateTime start1,
                        LocalDateTime end1,
                        LocalDateTime start2,
                        LocalDateTime end2) {

                if (start1 == null || end1 == null || start2 == null || end2 == null) {
                        return false;
                }

                return start1.isBefore(end2) && start2.isBefore(end1);
        }

        private Mono<SlotEventPayload> currentSlotPayload(Integer id) {

                return redisStateService
                                .getSlot(id)
                                .switchIfEmpty(
                                                repository.findById(id)
                                                                .map(this::toSlotEventPayload));
        }

        private SlotEventPayload toSlotEventPayload(ParkingSlotDto dto) {

                return new SlotEventPayload(
                                dto.slotId(),
                                dto.locationId(),
                                dto.displayCode(),
                                dto.vehicleTypeId(),
                                dto.reservationClassId(),
                                dto.sensorId(),
                                dto.currentStatus(),
                                LocalDateTime.now());
        }

        private ParkingSlotDto toParkingSlotDto(SlotEventPayload payload) {

                return new ParkingSlotDto(
                                payload.slotId(),
                                payload.locationId(),
                                payload.displayCode(),
                                payload.vehicleTypeId(),
                                payload.reservationClassId(),
                                payload.sensorId(),
                                payload.currentStatus());
        }

        public Mono<SlotAvailabilityResponseDto> checkSlotAvailability(SlotAvailabilityRequestDto request) {
            
            LOGGER.info("Checking slot availability for location: {}, from: {}, to: {}", 
                        request.locationId(), request.fromDate(), request.toDate());

            if (request.locationId() == null || request.locationId().isBlank()) {
                return Mono.error(new IllegalArgumentException("Location ID is required."));
            }
            if (request.fromDate() == null || request.toDate() == null) {
                return Mono.error(new IllegalArgumentException("From date and to date are required."));
            }
            if (request.fromDate().isAfter(request.toDate()) || request.fromDate().equals(request.toDate())) {
                return Mono.error(new IllegalArgumentException("From date must be before to date."));
            }

            LocalDateTime rangeStart = request.fromDate();
            LocalDateTime rangeEnd = request.toDate();

            return redisStateService.getAllSlots()
                    .filter(slot -> request.locationId().equals(slot.locationId()))
                    .collectList()
                    .flatMap(slots -> {
                        if (slots.isEmpty()) {
                            LOGGER.debug("No slots found for location: {}", request.locationId());
                            return Mono.just(new SlotAvailabilityResponseDto(
                                    rangeStart, rangeEnd, List.of(), List.of()));
                        }

                        return Mono.zip(
                                redisStateService.getAllReservations().collectList(),
                                redisStateService.getAllSessions().collectList())
                                .flatMap(tuple -> {
                                    List<ReservationEventPayload> reservations = tuple.getT1();
                                    List<SessionEventPayload> sessions = tuple.getT2();

                                    Set<Integer> locationSlotIds = slots.stream()
                                            .map(SlotEventPayload::slotId)
                                            .collect(Collectors.toSet());

                                    List<ReservationEventPayload> relevantReservations = reservations.stream()
                                            .filter(r -> locationSlotIds.contains(r.slotId()))
                                            .filter(r -> ACTIVE_RESERVATION_STATUSES.contains(
                                                            r.reservationStatus() != null ? r.reservationStatus().toUpperCase() : ""))
                                            .toList();

                                    List<SessionEventPayload> relevantSessions = sessions.stream()
                                            .filter(s -> locationSlotIds.contains(s.slotId()))
                                            .filter(s -> ACTIVE_SESSION_STATUSES.contains(
                                                            s.sessionStatus() != null ? s.sessionStatus().toUpperCase() : ""))
                                            .toList();

                                    List<Integer> availableSlotIds = slots.stream()
                                            .filter(slot -> !isSlotFilled(slot, relevantReservations, relevantSessions, rangeStart, rangeEnd))
                                            .map(SlotEventPayload::slotId)
                                            .toList();

                                    List<SlotAvailabilityResponseDto.OverlappingTimingDto> overlappingTimings = findOverlappingTimings(
                                            relevantReservations, relevantSessions, rangeStart, rangeEnd);

                                    return Mono.just(new SlotAvailabilityResponseDto(
                                            rangeStart, rangeEnd, availableSlotIds, overlappingTimings));
                                });
                    })
                    .doOnSuccess(result -> LOGGER.debug("Completed slot availability check for location: {}", request.locationId()))
                    .doOnError(e -> LOGGER.error("Error checking slot availability for location {}: {}", request.locationId(), e.getMessage()));
        }

        private List<SlotAvailabilityResponseDto.OverlappingTimingDto> findOverlappingTimings(
                List<ReservationEventPayload> reservations,
                List<SessionEventPayload> sessions,
                LocalDateTime rangeStart,
                LocalDateTime rangeEnd) {

            List<SlotAvailabilityResponseDto.OverlappingTimingDto> overlappingTimings = new ArrayList<>();

            for (ReservationEventPayload reservation : reservations) {
                if (reservation.reservedAt() != null && reservation.expiresAt() != null) {
                    if (timeRangesOverlap(reservation.reservedAt(), reservation.expiresAt(), rangeStart, rangeEnd)) {
                        LocalDateTime overlapStart = reservation.reservedAt().isAfter(rangeStart) 
                                ? reservation.reservedAt() : rangeStart;
                        LocalDateTime overlapEnd = reservation.expiresAt().isBefore(rangeEnd) 
                                ? reservation.expiresAt() : rangeEnd;
                        
                        overlappingTimings.add(new SlotAvailabilityResponseDto.OverlappingTimingDto(overlapStart, overlapEnd));
                    }
                }
            }

            for (SessionEventPayload session : sessions) {
                LocalDateTime sessionStart = session.actualEntryTime() != null
                        ? session.actualEntryTime()
                        : session.expectedExitTime() != null
                                ? session.expectedExitTime().minusMinutes(
                                                session.durationMinutes() != null ? session.durationMinutes() : 60)
                                : rangeStart;
                LocalDateTime sessionEnd = session.actualExitTime() != null
                        ? session.actualExitTime()
                        : session.expectedExitTime() != null
                                ? session.expectedExitTime()
                                : rangeEnd;

                if (timeRangesOverlap(sessionStart, sessionEnd, rangeStart, rangeEnd)) {
                    LocalDateTime overlapStart = sessionStart.isAfter(rangeStart) ? sessionStart : rangeStart;
                    LocalDateTime overlapEnd = sessionEnd.isBefore(rangeEnd) ? sessionEnd : rangeEnd;
                    
                    overlappingTimings.add(new SlotAvailabilityResponseDto.OverlappingTimingDto(overlapStart, overlapEnd));
                }
            }

            return mergeOverlappingIntervals(overlappingTimings);
        }

        private List<SlotAvailabilityResponseDto.OverlappingTimingDto> mergeOverlappingIntervals(
                List<SlotAvailabilityResponseDto.OverlappingTimingDto> intervals) {

            if (intervals.isEmpty()) {
                return List.of();
            }

            intervals.sort(Comparator.comparing(SlotAvailabilityResponseDto.OverlappingTimingDto::from));

            List<SlotAvailabilityResponseDto.OverlappingTimingDto> merged = new ArrayList<>();
            SlotAvailabilityResponseDto.OverlappingTimingDto current = intervals.get(0);

            for (int i = 1; i < intervals.size(); i++) {
                SlotAvailabilityResponseDto.OverlappingTimingDto next = intervals.get(i);
                
                if (current.to().isAfter(next.from()) || current.to().equals(next.from())) {
                    LocalDateTime newEnd = current.to().isAfter(next.to()) ? current.to() : next.to();
                    current = new SlotAvailabilityResponseDto.OverlappingTimingDto(current.from(), newEnd);
                } else {
                    merged.add(current);
                    current = next;
                }
            }
            merged.add(current);

            return merged;
        }
}

error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/ParkingSessionService.java:com/example/Metropark/repo/UserRepository#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/ParkingSessionService.java
empty definition using pc, found symbol in pc: com/example/Metropark/repo/UserRepository#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 192
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/ParkingSessionService.java
text:
```scala
package com.example.Metropark.service;

import com.example.Metropark.dto.ParkingSessionDto;
import com.example.Metropark.repo.ParkingSessionRepository;
import com.example.Metropark.repo.UserRe@@pository;
import com.example.Metropark.repo.VehicleRepository;
import com.example.Metropark.repo.ParkingSlotRepository;
import com.example.Metropark.repo.ReservationRepository;

import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class ParkingSessionService {

    private final ParkingSessionRepository sessionRepository;
    private final VehicleRepository vehicleRepository;
    private final ParkingSlotRepository slotRepository;
    private final ReservationRepository reservationRepository;

    // Constructor Injection for all required repositories
    public ParkingSessionService(
            ParkingSessionRepository sessionRepository,
            VehicleRepository vehicleRepository,
            ParkingSlotRepository slotRepository,
            ReservationRepository reservationRepository) {
        this.sessionRepository = sessionRepository;
        this.vehicleRepository = vehicleRepository;
        this.slotRepository = slotRepository;
        this.reservationRepository = reservationRepository;
    }

    public Mono<Integer> createSession(ParkingSessionDto dto) {
        if (dto.slotId() == null || dto.userId() == null || dto.vehicleId() == null) {
            return Mono.error(new IllegalArgumentException("Slot ID, User ID, and Vehicle ID are required."));
        }

        // 1. Verify Vehicle is ACTIVE
        Mono<Boolean> checkVehicle = vehicleRepository.findById(dto.vehicleId())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Vehicle not found.")))
                .flatMap(vehicle -> {
                    if (!Boolean.TRUE.equals(vehicle.isActive())) {
                        return Mono.error(new IllegalStateException("Cannot start session: Vehicle is inactive."));
                    }
                    return Mono.just(true);
                });

        // 2. Verify Slot is AVAILABLE
        Mono<Boolean> checkSlot = slotRepository.findById(dto.slotId())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Slot not found.")))
                .flatMap(slot -> {
                    if (!"AVAILABLE".equalsIgnoreCase(slot.currentStatus())) {
                        return Mono.error(new IllegalStateException(
                                "Cannot start session: Parking slot is already " + slot.currentStatus() + "."));
                    }
                    return Mono.just(true);
                });

        // 3. Verify Reservation is RESERVED (Only if a reservationId was provided)
        Mono<Boolean> checkReservation = Mono.just(true); // Default to true if no reservation is passed
        if (dto.reservationId() != null) {
            checkReservation = reservationRepository.findById(dto.reservationId())
                    .switchIfEmpty(Mono.error(new IllegalArgumentException("Reservation not found.")))
                    .flatMap(res -> {
                        if (!"RESERVED".equalsIgnoreCase(res.reservationStatus())) {
                            return Mono.error(new IllegalStateException("Cannot start session: Reservation is "
                                    + res.reservationStatus() + ", not RESERVED."));
                        }
                        return Mono.just(true);
                    });
        }

        // 4. Verify User is ACTIVE (Assuming you have a standard UserRepository)
        
          Mono<Boolean> checkUser = userRepository.findById(dto.userId())
          .switchIfEmpty(Mono.error(new IllegalArgumentException("User not found.")))
          .flatMap(user -> {
          if (!"ACTIVE".equalsIgnoreCase(user.userStatus())) {
          return Mono.error(new IllegalStateException("Cannot start session: User account is inactive."));
          }
          return Mono.just(true);
          });
        

        // CHAIN THEM ALL TOGETHER
        // We use .then() to execute these checks sequentially. If any check fails, the
        // chain breaks immediately!
        return checkVehicle
                .then(checkSlot)
                .then(checkReservation)
                .then(checkUser) // Uncomment once your User repository is ready
                .then(Mono.defer(() -> {
                    // ALL CHECKS PASSED! Now we can safely build and insert the session.
                    String status = (dto.sessionStatus() == null || dto.sessionStatus().isBlank())
                            ? "CREATED"
                            : dto.sessionStatus().trim().toUpperCase();

                    String payment = (dto.paymentStatus() == null || dto.paymentStatus().isBlank())
                            ? "PENDING"
                            : dto.paymentStatus().trim().toUpperCase();

                    LocalDateTime now = LocalDateTime.now();

                    ParkingSessionDto cleanDto = new ParkingSessionDto(
                            dto.sessionId(), dto.reservationId(), dto.slotId(), dto.userId(),
                            dto.vehicleId(), dto.entryGateId(), dto.exitGateId(), status,
                            dto.actualEntryTime(), dto.actualExitTime(), dto.expectedExitTime(),
                            dto.durationMinutes(), payment, 1, now, now);

                    return sessionRepository.create(cleanDto);
                }));
    }

    public Flux<ParkingSessionDto> getAllSessions() {
        return sessionRepository.findAll();
    }

    public Mono<ParkingSessionDto> getSessionById(Integer id) {
        return sessionRepository.findById(id);
    }

    public Mono<Integer> updateSessionStatus(Integer id, String status, Integer currentVersion) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Session status cannot be empty."));
        }

        return sessionRepository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated == 0) {
                        return Mono.error(
                                new IllegalStateException("Update failed: Concurrency conflict or Session not found."));
                    }
                    return Mono.just(rowsUpdated);
                });
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: com/example/Metropark/repo/UserRepository#
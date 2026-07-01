error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/ReservationService.java:_empty_/ParkingSlotRepository#updateStatusWithOptimisticLock#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/ReservationService.java
empty definition using pc, found symbol in pc: _empty_/ParkingSlotRepository#updateStatusWithOptimisticLock#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1439
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/ReservationService.java
text:
```scala
package com.example.Metropark.service;

import com.example.Metropark.dto.ReservationDto;
import com.example.Metropark.repo.ParkingSlotRepository;
import com.example.Metropark.repo.ReservationRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class ReservationService {

    private final ReservationRepository reservationRepository;
    private final ParkingSlotRepository parkingSlotRepository;

    public ReservationService(ReservationRepository reservationRepository,
            ParkingSlotRepository parkingSlotRepository) {
        this.reservationRepository = reservationRepository;
        this.parkingSlotRepository = parkingSlotRepository;
    }

    public Mono<Integer> createReservation(ReservationDto dto) {
        if (dto.userId() == null || dto.slotId() == null) {
            return Mono.error(new IllegalArgumentException("User ID and Slot ID are strictly required."));
        }

        return parkingSlotRepository.findById(dto.slotId())
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Slot does not exist.")))

                .filter(slot -> "AVAILABLE".equalsIgnoreCase(slot.currentStatus()))
                .switchIfEmpty(Mono.error(new IllegalStateException("Slot not available.")))

                .flatMap(slot -> parkingSlotRepository.updateStatusWithO@@ptimisticLock(slot.slotId(), "RESERVED",
                        slot.slotVersion()))

                .filter(rowsUpdated -> rowsUpdated > 0)
                .switchIfEmpty(Mono.error(new IllegalStateException("Slot not available.")))

                .flatMap(rowsUpdated -> {
                    LocalDateTime now = LocalDateTime.now();
                    LocalDateTime expiry = (dto.expiresAt() != null) ? dto.expiresAt() : now.plusMinutes(30);
                    String status = (dto.queueEntryId() != null) ? "WAITING" : "RESERVED";

                    ReservationDto processedDto = new ReservationDto(
                            dto.reservationId(), dto.userId(), dto.slotId(), dto.queueEntryId(),
                            status, 1, now, expiry, now, now);

                    return reservationRepository.create(processedDto);
                });
    }

    public Flux<ReservationDto> getAllReservations() {
        return reservationRepository.findAll();
    }

    public Mono<ReservationDto> getReservationById(Integer id) {
        return reservationRepository.findById(id);
    }

    public Mono<Integer> updateStatus(Integer id, String status, Integer currentVersion) {
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }

        return reservationRepository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)
                .filter(rowsUpdated -> rowsUpdated > 0)
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Update failed: Concurrency conflict or Reservation not found. Please refresh and try again.")));
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/ParkingSlotRepository#updateStatusWithOptimisticLock#
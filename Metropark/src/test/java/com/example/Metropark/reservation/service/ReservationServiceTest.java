package com.example.Metropark.reservation.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.reservation.dto.ReservationDto;
import com.example.Metropark.reservation.repo.ReservationRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ReservationServiceTest {

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private ParkingSlotRepository parkingSlotRepository;

    @InjectMocks
    private ReservationService service;

    @Test
    void createReservationReservesSlotAndSavesReservation() {
        ReservationDto input = new ReservationDto(null, 1001, 5, null, null, null, null, null, null, null);
        when(parkingSlotRepository.reserveSlot(5)).thenReturn(Mono.just(1));
        when(reservationRepository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createReservation(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<ReservationDto> captor = ArgumentCaptor.forClass(ReservationDto.class);
        verify(reservationRepository).create(captor.capture());

        ReservationDto saved = captor.getValue();
        assertEquals(1001, saved.userId());
        assertEquals(5, saved.slotId());
        assertEquals("RESERVED", saved.reservationStatus());
        assertEquals(1, saved.reservationVersion());
        assertNotNull(saved.reservedAt());
        assertNotNull(saved.expiresAt());
        assertNotNull(saved.createdAt());
        assertNotNull(saved.updatedAt());
    }

    @Test
    void createReservationRejectsMissingRequiredIds() {
        ReservationDto input = new ReservationDto(null, null, 5, null, null, null, null, null, null, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createReservation(input).block());

        assertEquals("User ID and Slot ID are strictly required.", exception.getMessage());
    }

    @Test
    void createReservationFailsWhenSlotCannotBeReserved() {
        ReservationDto input = new ReservationDto(null, 1001, 5, null, null, null, null, null, null, null);
        when(parkingSlotRepository.reserveSlot(5)).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.createReservation(input).block());

        assertEquals("Slot is already reserved or does not exist.", exception.getMessage());
    }

    @Test
    void updateStatusReturnsRows() {
        when(reservationRepository.updateStatusWithOptimisticLock(1, "CONFIRMED", 2)).thenReturn(Mono.just(1));

        Integer rows = service.updateStatus(1, " confirmed ", 2).block();

        assertEquals(1, rows);
        verify(reservationRepository).updateStatusWithOptimisticLock(1, "CONFIRMED", 2);
    }

    @Test
    void updateStatusFailsWhenNoRowsAreUpdated() {
        when(reservationRepository.updateStatusWithOptimisticLock(1, "CONFIRMED", 2)).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updateStatus(1, "CONFIRMED", 2).block());

        assertEquals("Update failed: Concurrency conflict or Reservation not found. Please refresh and try again.",
                exception.getMessage());
    }
}

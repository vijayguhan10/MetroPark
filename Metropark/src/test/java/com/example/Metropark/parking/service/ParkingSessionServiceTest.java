package com.example.Metropark.parking.service;

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

import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.reservation.repo.ReservationRepository;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.repo.VehicleRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ParkingSessionServiceTest {

    @Mock
    private ParkingSessionRepository sessionRepository;

    @Mock
    private VehicleRepository vehicleRepository;

    @Mock
    private ParkingSlotRepository slotRepository;

    @Mock
    private ReservationRepository reservationRepository;

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private ParkingSessionService service;

    @Test
    void createSessionValidatesAndNormalizesFields() {
        ParkingSessionDto input = new ParkingSessionDto(
                null,
                null,
                5,
                "USR-1001",
                20,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        when(vehicleRepository.findById(20)).thenReturn(Mono.just(new VehicleDto(20, "USR-1001", "ABC123", 2, null, null, null, true, null, null)));
        when(slotRepository.findById(5)).thenReturn(Mono.just(new ParkingSlotDto(5, "LOC-1", "A-01", 2, 3, "SENSOR-1", "AVAILABLE")));
        when(userRepository.findById("USR-1001")).thenReturn(Mono.just(new UserDto("USR-1001", "user@example.com", "ACTIVE")));
        when(sessionRepository.hasActiveSession(20)).thenReturn(Mono.just(false));
        when(sessionRepository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createSession(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<ParkingSessionDto> captor = ArgumentCaptor.forClass(ParkingSessionDto.class);
        verify(sessionRepository).create(captor.capture());

        ParkingSessionDto saved = captor.getValue();
        assertEquals(5, saved.slotId());
        assertEquals("USR-1001", saved.userId());
        assertEquals(20, saved.vehicleId());
        assertEquals("CREATED", saved.sessionStatus());
        assertEquals("PENDING", saved.paymentStatus());
        assertEquals(1, saved.sessionVersion());
        assertNotNull(saved.createdAt());
        assertNotNull(saved.updatedAt());
    }

    @Test
    void createSessionRejectsMissingRequiredFields() {
        ParkingSessionDto input = new ParkingSessionDto(
                null,
                null,
                5,
                null,
                20,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createSession(input).block());

        assertEquals("Slot ID, User ID, and Vehicle ID are required.", exception.getMessage());
    }

    @Test
    void createSessionFailsWhenVehicleAlreadyHasActiveSession() {
        ParkingSessionDto input = new ParkingSessionDto(
                null,
                null,
                5,
                "USR-1001",
                20,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null);

        when(vehicleRepository.findById(20)).thenReturn(Mono.just(new VehicleDto(20, "USR-1001", "ABC123", 2, null, null, null, true, null, null)));
        when(slotRepository.findById(5)).thenReturn(Mono.just(new ParkingSlotDto(5, "LOC-1", "A-01", 2, 3, "SENSOR-1", "AVAILABLE")));
        when(userRepository.findById("USR-1001")).thenReturn(Mono.just(new UserDto("USR-1001", "user@example.com", "ACTIVE")));
        when(sessionRepository.hasActiveSession(20)).thenReturn(Mono.just(true));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.createSession(input).block());

        assertEquals("Cannot start session: This vehicle is already parked or has a pending entry in another location.",
                exception.getMessage());
    }

    @Test
    void updateSessionStatusReturnsRows() {
        when(sessionRepository.updateStatusWithOptimisticLock(1, "ACTIVE", 2)).thenReturn(Mono.just(1));

        Integer rows = service.updateSessionStatus(1, "active", 2).block();

        assertEquals(1, rows);
        verify(sessionRepository).updateStatusWithOptimisticLock(1, "ACTIVE", 2);
    }

    @Test
    void updateSessionStatusFailsWhenNoRowsAreUpdated() {
        when(sessionRepository.updateStatusWithOptimisticLock(1, "ACTIVE", 2)).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updateSessionStatus(1, "active", 2).block());

        assertEquals("Concurrency conflict or session not found.", exception.getMessage());
    }
}

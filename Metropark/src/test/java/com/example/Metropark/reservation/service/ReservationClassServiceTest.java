package com.example.Metropark.reservation.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.repo.ReservationClassRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class ReservationClassServiceTest {

    @Mock
    private ReservationClassRepository repository;

    @InjectMocks
    private ReservationClassService service;

    @Test
    void createReservationClassConvertsToPascalCase() {
        ReservationClassDto input = new ReservationClassDto(null, "GENERAL");
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createReservationClass(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<ReservationClassDto> captor = ArgumentCaptor.forClass(ReservationClassDto.class);
        verify(repository).create(captor.capture());
        assertEquals("General", captor.getValue().className());
    }

    @Test
    void createReservationClassRejectsBlankName() {
        ReservationClassDto input = new ReservationClassDto(null, " ");

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createReservationClass(input).block());

        assertEquals("Class name cannot be empty.", exception.getMessage());
    }

    @Test
    void updateReservationClassConvertsToPascalCase() {
        ReservationClassDto input = new ReservationClassDto(null, "vip");
        when(repository.update(7, new ReservationClassDto(null, "Vip"))).thenReturn(Mono.just(1));

        Integer rows = service.updateReservationClass(7, input).block();

        assertEquals(1, rows);
        verify(repository).update(7, new ReservationClassDto(null, "Vip"));
    }

    @Test
    void deleteReservationClassDelegatesToRepository() {
        when(repository.delete(3)).thenReturn(Mono.just(1));

        Integer rows = service.deleteReservationClass(3).block();

        assertEquals(1, rows);
        verify(repository).delete(3);
    }
}

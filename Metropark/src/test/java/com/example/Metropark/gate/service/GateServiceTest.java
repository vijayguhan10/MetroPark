package com.example.Metropark.gate.service;

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

import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.gate.repo.GateRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class GateServiceTest {

    @Mock
    private GateRepository repository;

    @InjectMocks
    private GateService service;

    @Test
    void createGateNormalizesFields() {
        GateDto input = new GateDto(null, "LOC-1", " North Gate ", " entry ", null, null, null);
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createGate(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<GateDto> captor = ArgumentCaptor.forClass(GateDto.class);
        verify(repository).create(captor.capture());

        GateDto saved = captor.getValue();
        assertEquals("North Gate", saved.gateName());
        assertEquals("ENTRY", saved.gateType());
        assertEquals("ACTIVE", saved.status());
    }

    @Test
    void createGateRejectsMissingFields() {
        GateDto input = new GateDto(null, null, "Gate", "ENTRY", null, null, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createGate(input).block());

        assertEquals("Location ID is required.", exception.getMessage());
    }

    @Test
    void updateGateStatusReturnsRows() {
        when(repository.updateStatus(1, "ACTIVE")).thenReturn(Mono.just(1));

        Integer rows = service.updateGateStatus(1, " active ").block();

        assertEquals(1, rows);
        verify(repository).updateStatus(1, "ACTIVE");
    }

    @Test
    void updateGateStatusRejectsBlankStatus() {
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.updateGateStatus(1, " ").block());

        assertEquals("Status cannot be empty.", exception.getMessage());
    }

    @Test
    void updateGateStatusFailsWhenNotFound() {
        when(repository.updateStatus(1, "ACTIVE")).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updateGateStatus(1, "ACTIVE").block());

        assertEquals("Gate not found.", exception.getMessage());
    }
}

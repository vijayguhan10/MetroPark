package com.example.Metropark.location.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class LocationServiceTest {

    @Mock
    private LocationRepository repository;

    @InjectMocks
    private LocationService service;

    @Test
    void createLocationGeneratesIdAndDefaultsStatus() {
        LocationDto input = new LocationDto(null, 1, "Central Lot", "Metro City", null);
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createLocation(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<LocationDto> captor = ArgumentCaptor.forClass(LocationDto.class);
        verify(repository).create(captor.capture());

        LocationDto saved = captor.getValue();
        assertTrue(saved.locationId().matches("LOC-\\d{3}"));
        assertEquals(1, saved.typeId());
        assertEquals("Central Lot", saved.locationName());
        assertEquals("Metro City", saved.city());
        assertEquals("ACTIVE", saved.status());
    }

    @Test
    void updateLocationStatusReturnsRows() {
        when(repository.updateStatus("LOC-1", "ACTIVE")).thenReturn(Mono.just(1));

        Integer rows = service.updateLocationStatus("LOC-1", "ACTIVE").block();

        assertEquals(1, rows);
        verify(repository).updateStatus("LOC-1", "ACTIVE");
    }
}

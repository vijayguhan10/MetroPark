package com.example.Metropark.location.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.location.dto.LocationTypeDto;
import com.example.Metropark.location.repo.LocationTypeRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class LocationTypeServiceTest {

    @Mock
    private LocationTypeRepository repository;

    @InjectMocks
    private LocationTypeService service;

    @Test
    void createLocationTypeDelegatesToRepository() {
        LocationTypeDto input = new LocationTypeDto(null, "Campus");
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createLocationType(input).block();

        assertEquals(1, rows);
        verify(repository).create(input);
    }

    @Test
    void updateLocationTypeDelegatesToRepository() {
        LocationTypeDto input = new LocationTypeDto(1L, "Campus");
        when(repository.update(1, input)).thenReturn(Mono.just(1));

        Integer rows = service.updateLocationType(1, input).block();

        assertEquals(1, rows);
        verify(repository).update(1, input);
    }

    @Test
    void deleteLocationTypeDelegatesToRepository() {
        when(repository.delete(1)).thenReturn(Mono.just(1));

        Integer rows = service.deleteLocationType(1).block();

        assertEquals(1, rows);
        verify(repository).delete(1);
    }
}

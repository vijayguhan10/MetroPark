package com.example.Metropark.queue.service;

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

import com.example.Metropark.queue.dto.QueueDto;
import com.example.Metropark.queue.repo.QueueRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class QueueServiceTest {

    @Mock
    private QueueRepository repository;

    @InjectMocks
    private QueueService service;

    @Test
    void createQueueTrimsAndDefaultsStatus() {
        QueueDto input = new QueueDto(null, "LOC-1", 2, 3, "  Main Queue  ", null, null);
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createQueue(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<QueueDto> captor = ArgumentCaptor.forClass(QueueDto.class);
        verify(repository).create(captor.capture());

        QueueDto saved = captor.getValue();
        assertEquals("Main Queue", saved.queueName());
        assertEquals("ACTIVE", saved.status());
    }

    @Test
    void createQueueRejectsMissingRequiredFields() {
        QueueDto input = new QueueDto(null, null, 2, 3, "Main Queue", null, null);

        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> service.createQueue(input).block());

        assertEquals("Location ID and Queue Name are strictly required.", exception.getMessage());
    }

    @Test
    void updateQueueStatusReturnsRows() {
        when(repository.updateStatus(1, "ACTIVE")).thenReturn(Mono.just(1));

        Integer rows = service.updateQueueStatus(1, "active").block();

        assertEquals(1, rows);
        verify(repository).updateStatus(1, "ACTIVE");
    }

    @Test
    void updateQueueStatusFailsWhenNoRowsAreUpdated() {
        when(repository.updateStatus(1, "ACTIVE")).thenReturn(Mono.just(0));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updateQueueStatus(1, "ACTIVE").block());

        assertEquals("Update failed: Queue not found.", exception.getMessage());
    }
}

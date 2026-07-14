package com.example.Metropark.payments.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.repo.PaymentMethodRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PaymentMethodServiceTest {

    @Mock
    private PaymentMethodRepository repository;

    @InjectMocks
    private PaymentMethodService service;

    @Test
    void createPaymentMethodNormalizesAndDelegates() {
        PaymentMethodDto input = new PaymentMethodDto(null, " credit_card ", null, null);
        when(repository.existsByMethodName("CREDIT_CARD")).thenReturn(Mono.just(false));
        when(repository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createPaymentMethod(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<PaymentMethodDto> captor = ArgumentCaptor.forClass(PaymentMethodDto.class);
        verify(repository).create(captor.capture());

        PaymentMethodDto saved = captor.getValue();
        assertEquals("CREDIT_CARD", saved.methodName());
        assertEquals(Boolean.TRUE, saved.isActive());
        assertEquals(null, saved.methodId());
    }

    @Test
    void createPaymentMethodRejectsDuplicates() {
        PaymentMethodDto input = new PaymentMethodDto(null, "credit_card", null, null);
        when(repository.existsByMethodName("CREDIT_CARD")).thenReturn(Mono.just(true));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.createPaymentMethod(input).block());

        assertEquals("Payment method already exists.", exception.getMessage());
    }

    @Test
    void updatePaymentMethodUpdatesWhenNoConflictExists() {
        PaymentMethodDto input = new PaymentMethodDto(null, " upi ", false, null);
        when(repository.findByMethodName("UPI")).thenReturn(Mono.empty());
        when(repository.update(eq(5L), any())).thenReturn(Mono.just(1));

        Integer rows = service.updatePaymentMethod(5L, input).block();

        assertEquals(1, rows);
        ArgumentCaptor<PaymentMethodDto> captor = ArgumentCaptor.forClass(PaymentMethodDto.class);
        verify(repository).update(eq(5L), captor.capture());

        PaymentMethodDto saved = captor.getValue();
        assertEquals(5L, saved.methodId());
        assertEquals("UPI", saved.methodName());
        assertEquals(Boolean.FALSE, saved.isActive());
    }

    @Test
    void requireActiveMethodRejectsDisabledMethods() {
        when(repository.findById(1L)).thenReturn(Mono.just(new PaymentMethodDto(1L, "CREDIT_CARD", false, null)));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.requireActiveMethod(1).block());

        assertEquals("Payment method is disabled.", exception.getMessage());
    }
}

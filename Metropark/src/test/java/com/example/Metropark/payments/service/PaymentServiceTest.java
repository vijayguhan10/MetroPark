package com.example.Metropark.payments.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.repo.PaymentRepository;
import com.example.Metropark.testsupport.TestFixtures;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private PaymentRepository paymentRepository;

    @Mock
    private PaymentAuditLogger auditLogger;

    @Mock
    private PaymentMethodService paymentMethodService;

    @Mock
    private ParkingSessionRepository sessionRepository;

    @InjectMocks
    private PaymentService service;

    @Test
    void createPaymentCreatesPaymentWithSessionUserId() {
        PaymentDto input = new PaymentDto(
                null,
                " TX-100 ",
                10,
                null,
                1,
                new BigDecimal("15.50"),
                " usd ",
                null,
                null,
                null,
                null,
                null,
                null);

        when(paymentMethodService.requireActiveMethod(1)).thenReturn(Mono.empty());
        when(sessionRepository.findById(10)).thenReturn(Mono.just(TestFixtures.parkingSessionDto()));
        when(paymentRepository.existsByTransactionReference("TX-100")).thenReturn(Mono.just(false));
        when(paymentRepository.create(any())).thenReturn(Mono.just(1));

        Integer rows = service.createPayment(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<PaymentDto> captor = ArgumentCaptor.forClass(PaymentDto.class);
        verify(paymentRepository).create(captor.capture());

        PaymentDto saved = captor.getValue();
        assertEquals("TX-100", saved.transactionReference());
        assertEquals(10, saved.sessionId());
        assertEquals("USR-1001", saved.userId());
        assertEquals(1, saved.methodId());
        assertEquals(new BigDecimal("15.50"), saved.amount());
        assertEquals("USD", saved.currency());
        assertEquals("PENDING", saved.paymentStatus());
        assertNotNull(saved.createdAt());
        assertNotNull(saved.updatedAt());
        verify(auditLogger).logPaymentCreated(saved, 1);
    }

    @Test
    void createPaymentRejectsDuplicateTransactionReference() {
        PaymentDto input = new PaymentDto(
                null,
                "TX-100",
                10,
                null,
                1,
                new BigDecimal("15.50"),
                "USD",
                null,
                null,
                null,
                null,
                null,
                null);

        when(paymentMethodService.requireActiveMethod(1)).thenReturn(Mono.empty());
        when(sessionRepository.findById(10)).thenReturn(Mono.just(TestFixtures.parkingSessionDto()));
        when(paymentRepository.existsByTransactionReference("TX-100")).thenReturn(Mono.just(true));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.createPayment(input).block());

        assertEquals("Transaction reference already exists.", exception.getMessage());
    }

    @Test
    void updatePaymentStatusUpdatesAndAudits() {
        PaymentDto existing = TestFixtures.paymentDto();
        PaymentStatusUpdateDto input = new PaymentStatusUpdateDto("success", "admin", "Approved", "GW-1");

        when(paymentRepository.findById(1L)).thenReturn(Mono.just(existing));
        when(paymentRepository.updateStatus(eq(1L), eq("PENDING"), eq("SUCCESS"), any(), any())).thenReturn(Mono.just(1));

        Integer rows = service.updatePaymentStatus(1L, input).block();

        assertEquals(1, rows);
        verify(paymentRepository).updateStatus(eq(1L), eq("PENDING"), eq("SUCCESS"), any(), any());
        verify(auditLogger).logPaymentStatusUpdated(1L, "PENDING", "SUCCESS", "ADMIN", "Approved", "GW-1");
    }

    @Test
    void updatePaymentStatusRejectsInvalidTransition() {
        PaymentDto existing = new PaymentDto(
                1L,
                "TX-100",
                10,
                "USR-1001",
                1,
                new BigDecimal("15.50"),
                "USD",
                "FAILED",
                null,
                null,
                null,
                LocalDateTime.of(2024, 1, 15, 10, 30),
                LocalDateTime.of(2024, 1, 15, 10, 30));
        PaymentStatusUpdateDto input = new PaymentStatusUpdateDto("SUCCESS", "ADMIN", "Approved", "GW-1");

        when(paymentRepository.findById(1L)).thenReturn(Mono.just(existing));
        when(paymentRepository.updateStatus(any(), any(), any(), any(), any())).thenReturn(Mono.just(1));

        IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> service.updatePaymentStatus(1L, input).block());

        assertEquals("Invalid payment status transition from FAILED to SUCCESS.", exception.getMessage());
    }
}

error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/Payments/service/PaymentService.java:java/lang/String#isBlank().
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/Payments/service/PaymentService.java
empty definition using pc, found symbol in pc: java/lang/String#isBlank().
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 5567
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/Payments/service/PaymentService.java
text:
```scala
package com.example.Metropark.payments.service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import org.springframework.stereotype.Service;

import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentGatewayUpdateDto;
import com.example.Metropark.payments.dto.PaymentHistoryDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.repo.PaymentRepository;
import com.example.Metropark.reservation.repo.ReservationRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class PaymentService {

    private static final Set<String> ALLOWED_STATUSES = Set.of(
            "PENDING", "PROCESSING", "SUCCESS", "FAILED", "CANCELLED", "REFUNDED");
    private static final Set<String> FINAL_STATUSES = Set.of("SUCCESS", "FAILED", "CANCELLED", "REFUNDED");
    private static final Set<String> ALLOWED_CHANGED_BY = Set.of("USER", "SYSTEM", "GATEWAY", "ADMIN");
    private static final Map<String, Set<String>> STATUS_TRANSITIONS = Map.of(
            "PENDING", Set.of("PROCESSING", "SUCCESS", "FAILED", "CANCELLED"),
            "PROCESSING", Set.of("SUCCESS", "FAILED", "CANCELLED"),
            "SUCCESS", Set.of("REFUNDED"),
            "FAILED", Set.of(),
            "CANCELLED", Set.of(),
            "REFUNDED", Set.of());

    private final PaymentRepository paymentRepository;
    private final PaymentHistoryService historyService;
    private final PaymentMethodService paymentMethodService;
    private final ReservationRepository reservationRepository;
    private final ParkingSessionRepository sessionRepository;

    public PaymentService(
            PaymentRepository paymentRepository,
            PaymentHistoryService historyService,
            PaymentMethodService paymentMethodService,
            ReservationRepository reservationRepository,
            ParkingSessionRepository sessionRepository) {

        this.paymentRepository = paymentRepository;
        this.historyService = historyService;
        this.paymentMethodService = paymentMethodService;
        this.reservationRepository = reservationRepository;
        this.sessionRepository = sessionRepository;
    }

    public Mono<Integer> createPayment(PaymentDto dto) {
        PaymentDto cleanDto = normalize(dto, null);

        Mono<Void> reservationValidation = cleanDto.reservationId() == null
                ? Mono.empty()
                : reservationRepository.findById(cleanDto.reservationId())
                        .switchIfEmpty(Mono.error(new IllegalArgumentException("Reservation not found.")))
                        .then();

        Mono<Void> sessionValidation = cleanDto.sessionId() == null
                ? Mono.empty()
                : sessionRepository.findById(cleanDto.sessionId())
                        .switchIfEmpty(Mono.error(new IllegalArgumentException("Parking session not found.")))
                        .then();

        Mono<Void> referenceValidation = cleanDto.transactionReference() == null
                ? Mono.empty()
                : paymentRepository.existsByTransactionReference(cleanDto.transactionReference())
                        .flatMap(exists -> exists
                                ? Mono.error(new IllegalStateException("Transaction reference already exists."))
                                : Mono.empty());

        Mono<Void> methodValidation = paymentMethodService.requireActiveMethod(cleanDto.methodId()).then();

        return Mono.when(reservationValidation, sessionValidation, referenceValidation, methodValidation)
                .then(Mono.defer(() -> {
                    if (cleanDto.reservationId() == null && cleanDto.sessionId() == null) {
                        return Mono.error(
                                new IllegalArgumentException("Either reservation ID or session ID is required."));
                    }

                    return paymentRepository.create(cleanDto)
                            .flatMap(rows -> paymentRepository
                                    .findByTransactionReference(cleanDto.transactionReference())
                                    .switchIfEmpty(Mono.error(new IllegalStateException(
                                            "Payment was created but could not be reloaded for history tracking.")))
                                    .flatMap(savedPayment -> historyService.createHistory(new PaymentHistoryDto(
                                            null,
                                            savedPayment.paymentId(),
                                            null,
                                            savedPayment.paymentStatus(),
                                            "SYSTEM",
                                            "Payment created",
                                            savedPayment.transactionReference(),
                                            LocalDateTime.now()))
                                            .thenReturn(rows)));
                }));
    }

    public Flux<PaymentDto> getAllPayments() {
        return paymentRepository.findAll();
    }

    public Mono<PaymentDto> getPaymentById(Long id) {
        return paymentRepository.findById(id);
    }

    public Mono<PaymentDto> getPaymentByTransactionReference(String transactionReference) {
        if (transactionReference == null || transactionReference.@@isBlank()) {
            return Mono.error(new IllegalArgumentException("Transaction reference is required."));
        }
        return paymentRepository.findByTransactionReference(transactionReference.trim());
    }

    public Flux<PaymentDto> getPaymentsByReservationId(Integer reservationId) {
        return paymentRepository.findByReservationId(reservationId);
    }

    public Flux<PaymentDto> getPaymentsBySessionId(Integer sessionId) {
        return paymentRepository.findBySessionId(sessionId);
    }

    public Mono<Integer> updatePaymentStatus(Long id, PaymentStatusUpdateDto dto) {
        if (dto.status() == null || dto.status().isBlank()) {
            return Mono.error(new IllegalArgumentException("Payment status is required."));
        }
        if (dto.changedBy() == null || dto.changedBy().isBlank()) {
            return Mono.error(new IllegalArgumentException("changedBy is required."));
        }

        String normalizedStatus = dto.status().trim().toUpperCase();
        String changedBy = dto.changedBy().trim().toUpperCase();

        if (!ALLOWED_STATUSES.contains(normalizedStatus)) {
            return Mono.error(new IllegalArgumentException("Invalid payment status."));
        }
        if (!ALLOWED_CHANGED_BY.contains(changedBy)) {
            return Mono.error(new IllegalArgumentException("changedBy must be USER, SYSTEM, GATEWAY, or ADMIN."));
        }

        return paymentRepository.findById(id)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Payment not found.")))
                .flatMap(existing -> validateTransition(existing.paymentStatus(), normalizedStatus)
                        .then(paymentRepository.updateStatus(
                                id,
                                existing.paymentStatus(),
                                normalizedStatus,
                                FINAL_STATUSES.contains(normalizedStatus) ? LocalDateTime.now()
                                        : existing.processedAt(),
                                LocalDateTime.now())
                                .flatMap(rows -> rows == 0
                                        ? Mono.error(new IllegalStateException(
                                                "Payment update failed due to a concurrent change."))
                                        : historyService.createHistory(new PaymentHistoryDto(
                                                null,
                                                id,
                                                existing.paymentStatus(),
                                                normalizedStatus,
                                                changedBy,
                                                dto.reason(),
                                                dto.gatewayReference(),
                                                LocalDateTime.now())).thenReturn(rows))));
    }

    public Mono<Integer> updateGatewayResponse(Long id, PaymentGatewayUpdateDto dto) {
        String normalizedStatus = dto.paymentStatus().trim().toUpperCase();
        String changedBy = dto.changedBy() == null || dto.changedBy().isBlank()
                ? "GATEWAY"
                : dto.changedBy().trim().toUpperCase();

        if (!ALLOWED_STATUSES.contains(normalizedStatus)) {
            return Mono.error(new IllegalArgumentException("Invalid payment status."));
        }
        if (!ALLOWED_CHANGED_BY.contains(changedBy)) {
            return Mono.error(new IllegalArgumentException("changedBy must be USER, SYSTEM, GATEWAY, or ADMIN."));
        }

        if (dto.transactionReference() != null && !dto.transactionReference().isBlank()) {
            return paymentRepository.existsByTransactionReference(dto.transactionReference().trim())
                    .flatMap(exists -> exists
                            ? Mono.error(new IllegalStateException("Transaction reference already exists."))
                            : applyGatewayUpdate(id, dto, normalizedStatus, changedBy));
        }

        return applyGatewayUpdate(id, dto, normalizedStatus, changedBy);
    }

    private Mono<Integer> applyGatewayUpdate(
            Long id,
            PaymentGatewayUpdateDto dto,
            String normalizedStatus,
            String changedBy) {

        return paymentRepository.findById(id)
                .switchIfEmpty(Mono.error(new IllegalArgumentException("Payment not found.")))
                .flatMap(existing -> validateTransition(existing.paymentStatus(), normalizedStatus)
                        .then(paymentRepository.updateGatewayResponse(
                                id,
                                existing.paymentStatus(),
                                normalizedStatus,
                                dto.transactionReference() == null || dto.transactionReference().isBlank()
                                        ? existing.transactionReference()
                                        : dto.transactionReference().trim(),
                                dto.gatewayResponseCode().trim(),
                                dto.gatewayResponseMessage(),
                                dto.processedAt() == null ? LocalDateTime.now() : dto.processedAt(),
                                LocalDateTime.now())
                                .flatMap(rows -> rows == 0
                                        ? Mono.error(new IllegalStateException(
                                                "Payment update failed due to a concurrent change."))
                                        : historyService.createHistory(new PaymentHistoryDto(
                                                null,
                                                id,
                                                existing.paymentStatus(),
                                                normalizedStatus,
                                                changedBy,
                                                dto.reason(),
                                                dto.transactionReference(),
                                                LocalDateTime.now())).thenReturn(rows))));
    }

    private Mono<Void> validateTransition(String currentStatus, String nextStatus) {
        String current = currentStatus == null ? "PENDING" : currentStatus.trim().toUpperCase();
        String next = nextStatus.trim().toUpperCase();
        Set<String> allowed = STATUS_TRANSITIONS.getOrDefault(current, Set.of());

        if (current.equals(next)) {
            return Mono.empty();
        }
        if (!allowed.contains(next)) {
            return Mono.error(new IllegalStateException(
                    "Invalid payment status transition from " + current + " to " + next + "."));
        }
        return Mono.empty();
    }

    private PaymentDto normalize(PaymentDto dto, Long id) {
        if (dto.methodId() == null) {
            throw new IllegalArgumentException("Payment method ID is required.");
        }
        if (dto.amount().compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Amount must be greater than zero.");
        }
        if (dto.currency().isBlank() || dto.currency().trim().length() != 3) {
            throw new IllegalArgumentException("Currency must be a 3-letter ISO code.");
        }

        String status = dto.paymentStatus() == null || dto.paymentStatus().isBlank()
                ? "PENDING"
                : dto.paymentStatus().trim().toUpperCase();
        if (!ALLOWED_STATUSES.contains(status)) {
            throw new IllegalArgumentException("Invalid payment status.");
        }

        String normalizedReference = dto.transactionReference() == null || dto.transactionReference().isBlank()
                ? null
                : dto.transactionReference().trim();

        if (normalizedReference == null) {
            throw new IllegalArgumentException("Transaction reference is required for non-cash payments.");
        }

        LocalDateTime now = LocalDateTime.now();
        LocalDateTime processedAt = dto.processedAt();
        if (processedAt == null && FINAL_STATUSES.contains(status)) {
            processedAt = now;
        }

        return new PaymentDto(
                id != null ? id : dto.paymentId(),
                normalizedReference,
                dto.reservationId(),
                dto.sessionId(),
                dto.methodId(),
                dto.amount(),
                dto.currency().trim().toUpperCase(),
                status,
                dto.gatewayResponseCode(),
                dto.gatewayResponseMessage(),
                processedAt,
                dto.createdAt() == null ? now : dto.createdAt(),
                now);
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: java/lang/String#isBlank().
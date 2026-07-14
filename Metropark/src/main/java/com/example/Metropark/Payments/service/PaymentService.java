package com.example.Metropark.payments.service;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.repo.PaymentRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class PaymentService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentService.class);

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
    private final PaymentAuditLogger auditLogger;
    private final PaymentMethodService paymentMethodService;
    private final ParkingSessionRepository sessionRepository;

    public PaymentService(
            PaymentRepository paymentRepository,
            PaymentAuditLogger auditLogger,
            PaymentMethodService paymentMethodService,
            ParkingSessionRepository sessionRepository) {

        this.paymentRepository = paymentRepository;
        this.auditLogger = auditLogger;
        this.paymentMethodService = paymentMethodService;
        this.sessionRepository = sessionRepository;
    }

    @Transactional
    public Mono<Integer> createPayment(PaymentDto dto) {
        LOGGER.info("Creating payment: {}", dto);
        PaymentDto cleanDto = normalize(dto, null);

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

        Mono<Void> methodValidation = paymentMethodService.requireActiveMethod(cleanDto.methodId());
        return Mono.when(sessionValidation, referenceValidation, methodValidation)
                .then(Mono.defer(() -> {
                    if (cleanDto.sessionId() == null) {
                        return Mono.error(new IllegalArgumentException("Session ID is required."));
                    }

                    // Fetch session to get user_id
                    return sessionRepository.findById(cleanDto.sessionId())
                            .switchIfEmpty(Mono.error(new IllegalArgumentException("Parking session not found.")))
                            .flatMap(session -> {
                                PaymentDto dtoWithUserId = new PaymentDto(
                                        cleanDto.paymentId(),
                                        cleanDto.transactionReference(),
                                        cleanDto.sessionId(),
                                        session.userId(),
                                        cleanDto.methodId(),
                                        cleanDto.amount(),
                                        cleanDto.currency(),
                                        cleanDto.paymentStatus(),
                                        cleanDto.gatewayResponseCode(),
                                        cleanDto.gatewayResponseMessage(),
                                        cleanDto.processedAt(),
                                        cleanDto.createdAt(),
                                        cleanDto.updatedAt());
                                return paymentRepository.create(dtoWithUserId)
                                        .doOnSuccess(rows -> {
                                            LOGGER.info("Payment created successfully, rows affected: {}", rows);
                                            auditLogger.logPaymentCreated(dtoWithUserId, rows);
                                        })
                                        .doOnError(e -> LOGGER.error("Error creating payment: {}", e.getMessage()));
                            });
                }));
    }

    public Flux<PaymentDto> getAllPayments() {
        LOGGER.debug("Fetching all payments");
        return paymentRepository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all payments successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all payments: {}", e.getMessage()));
    }

    public Mono<PaymentDto> getPaymentById(Long id) {
        LOGGER.debug("Fetching payment by id: {}", id);
        return paymentRepository.findById(id)
                .doOnSuccess(dto -> LOGGER.debug("Fetched payment: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching payment by id {}: {}", id, e.getMessage()));
    }

    public Mono<PaymentDto> getPaymentByTransactionReference(String transactionReference) {
        LOGGER.debug("Fetching payment by transaction reference: {}", transactionReference);
        if (transactionReference == null || transactionReference.isBlank()) {
            return Mono.error(new IllegalArgumentException("Transaction reference is required."));
        }
        return paymentRepository.findByTransactionReference(transactionReference.trim())
                .doOnSuccess(dto -> LOGGER.debug("Fetched payment by transaction reference: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching payment by transaction reference {}: {}",
                        transactionReference, e.getMessage()));
    }

    public Flux<PaymentDto> getPaymentsBySessionId(Integer sessionId) {
        LOGGER.debug("Fetching payments by session id: {}", sessionId);
        return paymentRepository.findBySessionId(sessionId)
                .doOnComplete(() -> LOGGER.debug("Fetched payments by session id {} successfully", sessionId))
                .doOnError(
                        e -> LOGGER.error("Error fetching payments by session id {}: {}", sessionId, e.getMessage()));
    }

    @Transactional
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
                .flatMap(existing -> {
                    // Log before DB update
                    LOGGER.info(
                            "Payment status update initiated: paymentId={}, currentStatus={}, newStatus={}, changedBy={}, reason={}, gatewayReference={}",
                            id,
                            existing.paymentStatus(),
                            normalizedStatus,
                            changedBy,
                            dto.reason(),
                            dto.gatewayReference());

                    return validateTransition(existing.paymentStatus(), normalizedStatus)
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
                                            : Mono.fromRunnable(() -> auditLogger.logPaymentStatusUpdated(
                                                    id,
                                                    existing.paymentStatus(),
                                                    normalizedStatus,
                                                    changedBy,
                                                    dto.reason(),
                                                    dto.gatewayReference()))
                                                    .thenReturn(rows)));
                })
                .doOnSuccess(rows -> LOGGER.info("Payment status updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating payment status id {}: {}", id, e.getMessage()));
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
                dto.sessionId(),
                dto.userId(),
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

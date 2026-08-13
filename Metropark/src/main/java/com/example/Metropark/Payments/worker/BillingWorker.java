package com.example.Metropark.payments.worker;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.Event;
import com.example.Metropark.event.EventPublisher;
import com.example.Metropark.event.payload.BillingRequestedEventPayload;
import com.example.Metropark.event.payload.NotificationEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.repo.ParkingPricingRepository;
import com.example.Metropark.payments.repo.PaymentRepository;
import com.example.Metropark.payments.repo.WalletRepository;
import com.example.Metropark.redis.RedisStateService;
import com.example.Metropark.user.dto.SuspensionDto;
import com.example.Metropark.user.repo.SuspensionRepository;
import com.example.Metropark.user.repo.UserRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Mono;

@Component
public class BillingWorker {

    private static final Logger LOGGER = LoggerFactory.getLogger(BillingWorker.class);

    private final ParkingSessionRepository sessionRepository;
    private final PaymentRepository paymentRepository;
    private final WalletRepository walletRepository;
    private final ParkingPricingRepository pricingRepository;
    private final UserRepository userRepository;
    private final SuspensionRepository suspensionRepository;
    private final RedisStateService redisStateService;
    private final EventPublisher eventPublisher;
    private final ObjectMapper objectMapper;

    public BillingWorker(
            ParkingSessionRepository sessionRepository,
            PaymentRepository paymentRepository,
            WalletRepository walletRepository,
            ParkingPricingRepository pricingRepository,
            UserRepository userRepository,
            SuspensionRepository suspensionRepository,
            RedisStateService redisStateService,
            EventPublisher eventPublisher,
            ObjectMapper objectMapper) {
        this.sessionRepository = sessionRepository;
        this.paymentRepository = paymentRepository;
        this.walletRepository = walletRepository;
        this.pricingRepository = pricingRepository;
        this.userRepository = userRepository;
        this.suspensionRepository = suspensionRepository;
        this.redisStateService = redisStateService;
        this.eventPublisher = eventPublisher;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.BILLING_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void processBilling(
            Message message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable billing envelope");
            return;
        }

        BillingRequestedEventPayload payload = parsePayload(event.payload(), BillingRequestedEventPayload.class);
        if (payload == null || payload.sessionId() == null || payload.userId() == null) {
            deadLetter(channel, deliveryTag, "billing event carries missing session or user information");
            return;
        }

        Integer sessionId = payload.sessionId();
        String userId = payload.userId();

        LOGGER.info("BILLING WORKER RECEIVED | sessionId={} userId={}", sessionId, userId);

        loadSession(sessionId)
                .flatMap(session -> {
                    if ("PAID".equalsIgnoreCase(session.paymentStatus())
                            || "SUCCESS".equalsIgnoreCase(session.paymentStatus())
                            || "FAILED".equalsIgnoreCase(session.paymentStatus())) {
                        LOGGER.info("BILLING IDEMPOTENCY | session {} already processed with payment status {}",
                                sessionId, session.paymentStatus());
                        return Mono.empty();
                    }

                    return pricingRepository.getBaseRatePerHour()
                            .flatMap(baseRate -> {
                                BigDecimal surgeMultiplier = session.surgeMultiplier() != null ? session.surgeMultiplier() : BigDecimal.ONE;
                                int durationMinutes = session.durationMinutes() != null ? session.durationMinutes() : 0;
                                long hours = Math.max(1L, (long) Math.ceil(durationMinutes / 60.0));
                                BigDecimal amount = baseRate
                                        .multiply(BigDecimal.valueOf(hours))
                                        .multiply(surgeMultiplier)
                                        .setScale(2, RoundingMode.HALF_UP);

                                LOGGER.info("BILLING COMPUTED | session={} baseRate={} hours={} surge={} amount={}",
                                        sessionId, baseRate, hours, surgeMultiplier, amount);

                                return walletRepository.deductFund(userId, amount)
                                        .flatMap(rowsAffected -> {
                                            if (rowsAffected > 0) {
                                                return handlePaymentSuccess(session, amount, durationMinutes, userId, payload.vehicleId());
                                            } else {
                                                return handlePaymentFailure(session, amount, durationMinutes, userId, payload.vehicleId());
                                            }
                                        });
                            });
                })
                .subscribe(
                        ignored -> {},
                        error -> {
                            LOGGER.error("Billing processing failed for session {}", sessionId, error);
                            deadLetter(channel, deliveryTag, "billing calculation error for session " + sessionId);
                        },
                        () -> ack(channel, deliveryTag));
    }

    private Mono<Void> handlePaymentSuccess(
            SessionEventPayload session,
            BigDecimal amount,
            int durationMinutes,
            String userId,
            Integer vehicleId) {

        Integer sessionId = session.sessionId();
        LocalDateTime now = LocalDateTime.now();

        return paymentRepository.allocatePaymentId()
                .flatMap(paymentId -> {
                    String txnRef = "TXN-" + sessionId + "-" + paymentId;
                    PaymentDto paymentDto = new PaymentDto(
                            paymentId, txnRef, sessionId, userId, 1, amount, "INR", "SUCCESS",
                            "200", "Payment successful", now, now, now);

                    return paymentRepository.create(paymentDto, paymentId)
                            .then(sessionRepository.updatePaymentStatus(sessionId, "PAID", null))
                            .then(walletRepository.getFund(userId))
                            .flatMap(walletBalance -> {
                                String notificationId = "notif-" + UUID.randomUUID();
                                NotificationEventPayload notif = new NotificationEventPayload(
                                        notificationId, sessionId, userId, vehicleId, durationMinutes,
                                        amount, walletBalance, "SUCCESS", "ACTIVE", "Payment successful");

                                return eventPublisher.publishNotificationEvent(notif)
                                        .doOnSuccess(v -> LOGGER.info("BILLING SUCCESS | session={} userId={} amount={} remainingFund={}",
                                                sessionId, userId, amount, walletBalance));
                            });
                }).then();
    }

    private Mono<Void> handlePaymentFailure(
            SessionEventPayload session,
            BigDecimal amount,
            int durationMinutes,
            String userId,
            Integer vehicleId) {

        Integer sessionId = session.sessionId();
        LocalDateTime now = LocalDateTime.now();

        LOGGER.warn("BILLING INSUFFICIENT FUNDS | session={} userId={} amount={} -> SUSPENDING USER", sessionId, userId, amount);

        return paymentRepository.allocatePaymentId()
                .flatMap(paymentId -> {
                    String txnRef = "TXN-" + sessionId + "-" + paymentId;
                    PaymentDto paymentDto = new PaymentDto(
                            paymentId, txnRef, sessionId, userId, 1, amount, "INR", "FAILED",
                            "402", "Insufficient funds", now, now, now);

                    SuspensionDto suspensionDto = new SuspensionDto(
                            null, userId, "Payment failed due to insufficient funds",
                            now, null, true, "SYSTEM");

                    return paymentRepository.create(paymentDto, paymentId)
                            .then(sessionRepository.updatePaymentStatus(sessionId, "FAILED", "SUSPENDED"))
                            .then(userRepository.updateStatus(userId, "SUSPENDED"))
                            .then(suspensionRepository.createSuspension(suspensionDto))
                            .then(walletRepository.getFund(userId))
                            .flatMap(walletBalance -> {
                                String notificationId = "notif-" + UUID.randomUUID();
                                NotificationEventPayload notif = new NotificationEventPayload(
                                        notificationId, sessionId, userId, vehicleId, durationMinutes,
                                        amount, walletBalance, "FAILED", "SUSPENDED",
                                        "Your parking session ended but payment failed due to insufficient funds. Your account has been suspended.");

                                return eventPublisher.publishNotificationEvent(notif)
                                        .doOnSuccess(v -> LOGGER.info("BILLING FAILED | User {} suspended due to insufficient funds for session {}", userId, sessionId));
                            });
                }).then();
    }

    private Mono<SessionEventPayload> loadSession(Integer sessionId) {
        return redisStateService.getSession(sessionId)
                .switchIfEmpty(sessionRepository.findById(sessionId)
                        .map(session -> new SessionEventPayload(
                                session.sessionId(), session.reservationId(), session.slotId(),
                                session.userId(), session.vehicleId(), session.entryGateId(),
                                session.exitGateId(), session.sessionStatus(), session.actualEntryTime(),
                                session.actualExitTime(), session.expectedExitTime(), session.durationMinutes(),
                                session.paymentStatus(), session.surgeMultiplier(), session.sessionVersion(), session.updatedAt())));
    }

    private Event parseEvent(Message message) {
        try {
            return objectMapper.readValue(message.getBody(), Event.class);
        } catch (Exception e) {
            LOGGER.error("Failed to parse billing event", e);
            return null;
        }
    }

    private <T> T parsePayload(Object payload, Class<T> clazz) {
        try {
            if (payload == null) return null;
            if (clazz.isInstance(payload)) return clazz.cast(payload);
            return objectMapper.convertValue(payload, clazz);
        } catch (Exception e) {
            LOGGER.error("Failed to parse billing payload as {}", clazz.getSimpleName(), e);
            return null;
        }
    }

    private void ack(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOGGER.error("Failed to ack billing delivery {}", deliveryTag, e);
        }
    }

    private void deadLetter(Channel channel, long deliveryTag, String reason) {
        LOGGER.error("Dead lettering billing delivery {} ({})", deliveryTag, reason);
        try {
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            LOGGER.error("Failed to nack billing delivery {}", deliveryTag, e);
        }
    }
}

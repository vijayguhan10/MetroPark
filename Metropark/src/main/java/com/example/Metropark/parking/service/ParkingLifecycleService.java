package com.example.Metropark.parking.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.event.EventPublisher;
import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;
import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.payments.payment.repo.PaymentRepository;
import com.example.Metropark.redis.DistributedLockService;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Mono;

@Service
public class ParkingLifecycleService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParkingLifecycleService.class);
        private static final Random RANDOM = new Random();

        public static final String SLOT_AVAILABLE = "AVAILABLE";
        public static final String SLOT_OCCUPIED = "OCCUPIED";

        public static final String SESSION_CREATED = "CREATED";
        public static final String SESSION_EXITED = "EXITED";

        public static final String PAYMENT_PENDING = "PENDING";
        public static final String PAYMENT_SUCCESS = "SUCCESS";
        public static final String PAYMENT_FAILED = "FAILED";
        public static final String SESSION_PAYMENT_PAID = "PAID";
        public static final String SESSION_PAYMENT_FAILED = "FAILED";

        private static final int INITIAL_VERSION = 1;

        private static final int SLOT_CANDIDATES = 25;

        private static final double PAYMENT_PENDING_RATE = 0.3;
        private static final double PAYMENT_FAILURE_RATE = 0.1;

        private final ParkingSessionRepository sessionRepository;
        private final ParkingSlotRepository slotRepository;
        private final PaymentRepository paymentRepository;
        private final RedisStateService redisStateService;
        private final EventPublisher eventPublisher;
        private final DistributedLockService lockService;
        private final com.example.Metropark.user.repo.UserRepository userRepository;

        public ParkingLifecycleService(
                        ParkingSessionRepository sessionRepository,
                        ParkingSlotRepository slotRepository,
                        PaymentRepository paymentRepository,
                        RedisStateService redisStateService,
                        EventPublisher eventPublisher,
                        DistributedLockService lockService,
                        com.example.Metropark.user.repo.UserRepository userRepository) {

                this.sessionRepository = sessionRepository;
                this.slotRepository = slotRepository;
                this.paymentRepository = paymentRepository;
                this.redisStateService = redisStateService;
                this.eventPublisher = eventPublisher;
                this.lockService = lockService;
                this.userRepository = userRepository;
        }

        public record ParkedVehicle(
                        Integer sessionId,
                        Integer slotId,
                        Long paymentId,
                        String userId,
                        Integer vehicleId,
                        LocalDateTime entryTime) {
        }

        public Mono<ParkedVehicle> parkVehicle(
                        String userId,
                        Integer vehicleId,
                        Integer slotId,
                        Integer entryGateId,
                        Integer methodId,
                        BigDecimal amount,
                        String currency) {

                return userRepository.findById(userId)
                                .flatMap(user -> {
                                        if (user != null && "SUSPENDED".equalsIgnoreCase(user.userStatus())) {
                                                LOGGER.warn("ENTRY REJECTED | user {} is SUSPENDED", userId);
                                                return Mono.<ParkedVehicle>error(new IllegalStateException(
                                                                "Vehicle entry blocked: User " + userId
                                                                                + " is SUSPENDED."));
                                        }
                                        return Mono.empty();
                                })
                                .then(Mono.zip(sessionRepository.allocateSessionId(),
                                                paymentRepository.allocatePaymentId()))
                                .flatMap(ids -> {
                                        Integer sessionId = ids.getT1();
                                        Long paymentId = ids.getT2();
                                        LocalDateTime now = LocalDateTime.now();

                                        return currentSlot(slotId)
                                                        .flatMap(slot -> slotRepository
                                                                        .getOccupancyRate(slot.locationId())
                                                                        .map(occupancy -> {
                                                                                BigDecimal surgeMultiplier = occupancy > 0.80
                                                                                                ? new BigDecimal("1.50")
                                                                                                : new BigDecimal(
                                                                                                                "1.00");
                                                                                LOGGER.info("ENTRY SURGE CALCULATED | slot={} lot={} occupancy={} surge={}",
                                                                                                slotId,
                                                                                                slot.locationId(),
                                                                                                occupancy,
                                                                                                surgeMultiplier);
                                                                                return surgeMultiplier;
                                                                        })
                                                                        .flatMap(surgeMultiplier -> {
                                                                                SessionEventPayload session = new SessionEventPayload(
                                                                                                sessionId,
                                                                                                null,
                                                                                                slotId,
                                                                                                userId,
                                                                                                vehicleId,
                                                                                                entryGateId,
                                                                                                entryGateId,
                                                                                                SESSION_CREATED,
                                                                                                now,
                                                                                                null,
                                                                                                null,
                                                                                                0,
                                                                                                PAYMENT_PENDING,
                                                                                                surgeMultiplier,
                                                                                                INITIAL_VERSION,
                                                                                                now);

                                                                                PaymentEventPayload payment = new PaymentEventPayload(
                                                                                                paymentId,
                                                                                                "TXN-" + sessionId + "-"
                                                                                                                + paymentId,
                                                                                                sessionId,
                                                                                                userId,
                                                                                                methodId,
                                                                                                amount,
                                                                                                currency,
                                                                                                PAYMENT_PENDING,
                                                                                                null,
                                                                                                null,
                                                                                                null,
                                                                                                now);

                                                                                SlotEventPayload occupiedSlot = withStatus(
                                                                                                slot, SLOT_OCCUPIED,
                                                                                                now);

                                                                                return redisStateService
                                                                                                .saveSessionState(
                                                                                                                session,
                                                                                                                INITIAL_VERSION)
                                                                                                .flatMap(storedSession -> redisStateService
                                                                                                                .saveSlotState(occupiedSlot,
                                                                                                                                INITIAL_VERSION)
                                                                                                                .flatMap(storedSlot -> redisStateService
                                                                                                                                .savePaymentState(
                                                                                                                                                payment,
                                                                                                                                                INITIAL_VERSION)
                                                                                                                                .flatMap(storedPayment -> eventPublisher
                                                                                                                                                .publishVehicleEntry(
                                                                                                                                                                new ParkingLifecycleEventPayload(
                                                                                                                                                                                storedSession,
                                                                                                                                                                                storedSlot,
                                                                                                                                                                                storedPayment,
                                                                                                                                                                                null,
                                                                                                                                                                                INITIAL_VERSION),
                                                                                                                                                                INITIAL_VERSION))));
                                                                        }))
                                                        .thenReturn(new ParkedVehicle(sessionId, slotId, paymentId,
                                                                        userId, vehicleId, now));
                                })
                                .doOnSuccess(parked -> LOGGER.info(
                                                "ENTRY | session={} slot={} payment={} user={} vehicle={} written to Redis and published",
                                                parked.sessionId(), parked.slotId(), parked.paymentId(),
                                                parked.userId(),
                                                parked.vehicleId()));
        }

        public Mono<ParkedVehicle> parkVehicleAtLot(
                        String userId,
                        Integer vehicleId,
                        String parkingLotId,
                        Integer entryGateId,
                        Integer methodId,
                        BigDecimal amount,
                        String currency) {

                return allocateSlot(parkingLotId)
                                .switchIfEmpty(Mono.error(new IllegalStateException(
                                                "Cannot park vehicle " + vehicleId + ": no free slot at lot "
                                                                + parkingLotId)))
                                .flatMap(claim -> parkVehicle(
                                                userId, vehicleId, claim.slotId(), entryGateId, methodId, amount,
                                                currency)
                                                .doFinally(signal -> lockService.release(claim.lock()).subscribe()));
        }

        private record SlotClaim(Integer slotId, DistributedLockService.Lock lock) {
        }

        private Mono<SlotClaim> allocateSlot(String parkingLotId) {
                return slotRepository.findAvailableSlotIds(parkingLotId, SLOT_CANDIDATES)
                                .concatMap(this::claimIfStillFree)
                                .next();
        }

        private Mono<SlotClaim> claimIfStillFree(Integer slotId) {
                return redisStateService.getSlotStatus(slotId)
                                .defaultIfEmpty(SLOT_AVAILABLE)
                                .filter(SLOT_AVAILABLE::equalsIgnoreCase)
                                .flatMap(status -> lockService.claimSlot(slotId))
                                .map(lock -> new SlotClaim(slotId, lock));
        }

        public Mono<Void> exitVehicleByVehicleId(Integer vehicleId) {
                return sessionRepository.findActiveByVehicleId(vehicleId)
                                .switchIfEmpty(Mono.error(new IllegalStateException(
                                                "Cannot exit vehicle " + vehicleId + ": it holds no open session.")))
                                .flatMap(session -> exitVehicle(new ParkedVehicle(
                                                session.sessionId(),
                                                session.slotId(),
                                                null,
                                                session.userId(),
                                                session.vehicleId(),
                                                session.actualEntryTime())));
        }

        public Mono<Void> exitVehicle(ParkedVehicle parked) {
                Integer sessionId = parked.sessionId();

                return currentSession(sessionId)
                                .switchIfEmpty(Mono.error(new IllegalStateException(
                                                "Cannot exit: session " + sessionId
                                                                + " is unknown to Redis and to PostgreSQL.")))
                                .flatMap(session -> {
                                        int expectedVersion = session.sessionVersion() == null
                                                        ? INITIAL_VERSION
                                                        : session.sessionVersion();
                                        int newVersion = expectedVersion + 1;

                                        LocalDateTime now = LocalDateTime.now();
                                        LocalDateTime entryTime = session.actualEntryTime() == null
                                                        ? parked.entryTime()
                                                        : session.actualEntryTime();
                                        int durationMinutes = entryTime == null
                                                        ? 0
                                                        : (int) Duration.between(entryTime, now).toMinutes();

                                        BigDecimal surgeMultiplier = session.surgeMultiplier() == null
                                                        ? BigDecimal.ONE
                                                        : session.surgeMultiplier();

                                        SessionEventPayload exited = new SessionEventPayload(
                                                        session.sessionId(),
                                                        session.reservationId(),
                                                        session.slotId(),
                                                        session.userId(),
                                                        session.vehicleId(),
                                                        session.entryGateId(),
                                                        session.exitGateId(),
                                                        SESSION_EXITED,
                                                        entryTime,
                                                        now,
                                                        session.expectedExitTime(),
                                                        durationMinutes,
                                                        PAYMENT_PENDING,
                                                        surgeMultiplier,
                                                        newVersion,
                                                        now);

                                        Integer slotId = session.slotId() == null ? parked.slotId() : session.slotId();

                                        return currentSlot(slotId)
                                                        .map(slot -> withStatus(slot, SLOT_AVAILABLE, now))
                                                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                                                        "Cannot exit session " + sessionId + ": slot "
                                                                                        + slotId
                                                                                        + " is unknown to Redis and to PostgreSQL.")))
                                                        .flatMap(slot -> redisStateService
                                                                        .saveSessionState(exited, newVersion)
                                                                        .flatMap(storedSession -> redisStateService
                                                                                        .saveSlotState(slot, newVersion)
                                                                                        .flatMap(storedSlot -> eventPublisher
                                                                                                        .publishVehicleExit(
                                                                                                                        new ParkingLifecycleEventPayload(
                                                                                                                                        storedSession,
                                                                                                                                        storedSlot,
                                                                                                                                        null,
                                                                                                                                        expectedVersion,
                                                                                                                                        newVersion),
                                                                                                                        newVersion)
                                                                                                        .then(eventPublisher
                                                                                                                        .publishBillingRequested(
                                                                                                                                        new com.example.Metropark.event.payload.BillingRequestedEventPayload(
                                                                                                                                                        "evt-" + System.currentTimeMillis()
                                                                                                                                                                        + "-"
                                                                                                                                                                        + sessionId,
                                                                                                                                                        sessionId,
                                                                                                                                                        session.userId(),
                                                                                                                                                        session.vehicleId())))
                                                                                                        .doOnSuccess(ignored -> LOGGER
                                                                                                                        .info(
                                                                                                                                        "EXIT | session={} slot={} version {}->{} published VEHICLE_EXIT & BILLING_REQUESTED",
                                                                                                                                        sessionId,
                                                                                                                                        slotId,
                                                                                                                                        expectedVersion,
                                                                                                                                        newVersion)))));
                                })
                                .then();
        }

        private Mono<PaymentEventPayload> resolvePaidPayment(
                        ParkedVehicle parked,
                        Integer sessionId,
                        LocalDateTime now) {

                Mono<PaymentEventPayload> known = parked.paymentId() == null
                                ? Mono.empty()
                                : redisStateService.getPayment(parked.paymentId());

                return known
                                .switchIfEmpty(paymentRepository.findBySessionId(sessionId).next()
                                                .map(dto -> new PaymentEventPayload(
                                                                dto.paymentId(),
                                                                dto.transactionReference(),
                                                                dto.sessionId(),
                                                                dto.userId(),
                                                                dto.methodId(),
                                                                dto.amount(),
                                                                dto.currency(),
                                                                dto.paymentStatus(),
                                                                dto.gatewayResponseCode(),
                                                                dto.gatewayResponseMessage(),
                                                                dto.processedAt(),
                                                                now)));
        }

        private Mono<SessionEventPayload> currentSession(Integer sessionId) {
                return redisStateService.getSession(sessionId)
                                .switchIfEmpty(
                                                sessionRepository.findById(sessionId)
                                                                .map(session -> new SessionEventPayload(
                                                                                session.sessionId(),
                                                                                session.reservationId(),
                                                                                session.slotId(),
                                                                                session.userId(),
                                                                                session.vehicleId(),
                                                                                session.entryGateId(),
                                                                                session.exitGateId(),
                                                                                session.sessionStatus(),
                                                                                session.actualEntryTime(),
                                                                                session.actualExitTime(),
                                                                                session.expectedExitTime(),
                                                                                session.durationMinutes(),
                                                                                session.paymentStatus(),
                                                                                session.surgeMultiplier(),
                                                                                session.sessionVersion(),
                                                                                session.updatedAt())));
        }

        private Mono<SlotEventPayload> currentSlot(Integer slotId) {
                return redisStateService.getSlot(slotId)
                                .switchIfEmpty(slotRepository.findById(slotId)
                                                .map(slot -> new SlotEventPayload(
                                                                slot.slotId(),
                                                                slot.locationId(),
                                                                slot.displayCode(),
                                                                slot.vehicleTypeId(),
                                                                slot.reservationClassId(),
                                                                slot.sensorId(),
                                                                slot.currentStatus(),
                                                                LocalDateTime.now())));
        }

        private SlotEventPayload withStatus(SlotEventPayload slot, String status, LocalDateTime at) {
                return new SlotEventPayload(
                                slot.slotId(),
                                slot.locationId(),
                                slot.displayCode(),
                                slot.vehicleTypeId(),
                                slot.reservationClassId(),
                                slot.sensorId(),
                                status,
                                at);
        }

}

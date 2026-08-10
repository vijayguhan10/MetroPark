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
import com.example.Metropark.payments.repo.PaymentRepository;
import com.example.Metropark.redis.DistributedLockService;
import com.example.Metropark.redis.RedisStateService;

import reactor.core.publisher.Mono;

/**
 * One vehicle in, one vehicle out - as a single unit.
 *
 * <p>
 * Entry and exit each touch three tables, and the three have to move together:
 * a slot released while its session stays open, or a session closed while its
 * payment stays PENDING, is exactly the divergence the Redis-first design
 * exists to prevent. So each operation writes all three entities to Redis and
 * then publishes ONE {@link ParkingLifecycleEventPayload}, which
 * {@link com.example.Metropark.parking.repo.ParkingLifecycleRepository} applies
 * to PostgreSQL in one transaction.
 *
 * <p>
 * This sits alongside {@link ParkingSessionService} rather than inside it:
 * that class owns the per-entity REST operations, where callers legitimately
 * change a session without touching a slot or a payment.
 */
@Service
public class ParkingLifecycleService {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParkingLifecycleService.class);
        private static final Random RANDOM = new Random();

        /** Slot lifecycle: AVAILABLE -> OCCUPIED -> AVAILABLE, forever. */
        public static final String SLOT_AVAILABLE = "AVAILABLE";
        public static final String SLOT_OCCUPIED = "OCCUPIED";

        /**
         * Session lifecycle: a new session is CREATED and then EXITED. Never reused.
         */
        public static final String SESSION_CREATED = "CREATED";
        public static final String SESSION_EXITED = "EXITED";

        /**
         * Payment lifecycle: PENDING, then settled. The two tables spell "settled"
         * differently and the difference is enforced by CHECK constraints, so they must
         * not be collapsed into one constant:
         *
         * <ul>
         * <li>{@code parking_sessions.payment_status} allows PENDING / PAID / FAILED /
         * REFUNDED - so the session carries {@link #SESSION_PAYMENT_PAID}.
         * <li>{@code payments.payment_status} allows PENDING / PROCESSING / SUCCESS /
         * FAILED / CANCELLED / REFUNDED - so the payment row carries
         * {@link #PAYMENT_SUCCESS}. This is also the value AdminDashboardService and
         * UserParkingFrequencyService already filter revenue on.
         * </ul>
         *
         * Writing "PAID" into payments violates {@code chk_payments_status}, which
         * aborts the whole exit transaction and dead letters the event.
         */
        public static final String PAYMENT_PENDING = "PENDING";
        public static final String PAYMENT_SUCCESS = "SUCCESS";
        public static final String PAYMENT_FAILED = "FAILED";
        public static final String SESSION_PAYMENT_PAID = "PAID";
        public static final String SESSION_PAYMENT_FAILED = "FAILED";

        /**
         * First version assigned to a session, in Redis and in session_version alike.
         */
        private static final int INITIAL_VERSION = 1;

        /**
         * How many PostgreSQL-AVAILABLE slots to consider before giving up on an entry.
         * Every candidate costs a Redis read, and a lot with none free in its first
         * {@value} genuinely is full for this tick's purposes - the next camera event
         * tries again.
         */
        private static final int SLOT_CANDIDATES = 25;

        /**
         * Payment outcome probabilities on exit:
         * - 30% PENDING (payment still processing)
         * - 10% FAILED
         * - 60% SUCCESS
         */
        private static final double PAYMENT_PENDING_RATE = 0.3;
        private static final double PAYMENT_FAILURE_RATE = 0.1;
        // SUCCESS rate is implicit: 1.0 - PAYMENT_PENDING_RATE - PAYMENT_FAILURE_RATE = 0.6

        private final ParkingSessionRepository sessionRepository;
        private final ParkingSlotRepository slotRepository;
        private final PaymentRepository paymentRepository;
        private final RedisStateService redisStateService;
        private final EventPublisher eventPublisher;
        private final DistributedLockService lockService;

        public ParkingLifecycleService(
                        ParkingSessionRepository sessionRepository,
                        ParkingSlotRepository slotRepository,
                        PaymentRepository paymentRepository,
                        RedisStateService redisStateService,
                        EventPublisher eventPublisher,
                        DistributedLockService lockService) {

                this.sessionRepository = sessionRepository;
                this.slotRepository = slotRepository;
                this.paymentRepository = paymentRepository;
                this.redisStateService = redisStateService;
                this.eventPublisher = eventPublisher;
                this.lockService = lockService;
        }

        /**
         * Everything the exit side needs to close a parked vehicle out, returned by
         * {@link #parkVehicle} so the 2-second exit tick never has to search for it.
         */
        public record ParkedVehicle(
                        Integer sessionId,
                        Integer slotId,
                        Long paymentId,
                        String userId,
                        Integer vehicleId,
                        LocalDateTime entryTime) {
        }

        /**
         * Park exactly one vehicle: a NEW session (CREATED), the slot flipped to
         * OCCUPIED, and a NEW payment (PENDING).
         *
         * <p>
         * Both ids are drawn from the PostgreSQL sequences without inserting a row, so
         * Redis holds the entities under the very ids the consumer will later insert
         * under.
         */
        public Mono<ParkedVehicle> parkVehicle(
                        String userId,
                        Integer vehicleId,
                        Integer slotId,
                        Integer entryGateId,
                        Integer methodId,
                        BigDecimal amount,
                        String currency) {

                return Mono.zip(sessionRepository.allocateSessionId(), paymentRepository.allocatePaymentId())
                                .flatMap(ids -> {
                                        Integer sessionId = ids.getT1();
                                        Long paymentId = ids.getT2();
                                        LocalDateTime now = LocalDateTime.now();

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
                                                        INITIAL_VERSION,
                                                        now);

                                        PaymentEventPayload payment = new PaymentEventPayload(
                                                        paymentId,
                                                        "TXN-" + sessionId + "-" + paymentId,
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

                                        return currentSlot(slotId)
                                                        .map(slot -> withStatus(slot, SLOT_OCCUPIED, now))
                                                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                                                        "Cannot park: slot " + slotId
                                                                                        + " is unknown to Redis and to PostgreSQL.")))
                                                        // Redis first, all three, and only then the publish - built
                                                        // from what Redis actually stored.
                                                        .flatMap(slot -> redisStateService
                                                                        .saveSessionState(session, INITIAL_VERSION)
                                                                        .flatMap(storedSession -> redisStateService
                                                                                        .saveSlotState(slot,
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
                                                                                                                                        INITIAL_VERSION)))))
                                                        .thenReturn(new ParkedVehicle(sessionId, slotId, paymentId,
                                                                        userId, vehicleId, now));
                                })
                                .doOnSuccess(parked -> LOGGER.info(
                                                "ENTRY | session={} slot={} payment={} user={} vehicle={} written to Redis and published",
                                                parked.sessionId(), parked.slotId(), parked.paymentId(),
                                                parked.userId(),
                                                parked.vehicleId()));
        }

        /**
         * Park one vehicle when the caller knows WHERE but not WHICH slot - the
         * camera-driven path.
         *
         * <p>
         * A camera reports a lot, never a slot number, so the slot has to be chosen
         * here. This allocates one, delegates to {@link #parkVehicle} unchanged, and
         * releases the claim either way. It adds no parking logic of its own; it only
         * answers the question a camera cannot.
         */
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
                                                // The claim is released on BOTH paths. On success the slot is
                                                // already OCCUPIED in Redis so nothing else would take it
                                                // anyway; on failure, holding the claim to its TTL would
                                                // needlessly shrink the lot.
                                                .doFinally(signal -> lockService.release(claim.lock()).subscribe()));
        }

        /** A slot taken for an entry, and the claim proving it is this caller's. */
        private record SlotClaim(Integer slotId, DistributedLockService.Lock lock) {
        }

        /**
         * The first slot that is free in PostgreSQL, still free in Redis, and not
         * already claimed by a concurrent entry - all three, in that order.
         *
         * <p>
         * PostgreSQL supplies candidates but cannot decide: it lags behind by however
         * long the lifecycle consumer takes, so a slot it calls AVAILABLE may already
         * hold a car. Redis is the real-time answer. The claim then closes the gap
         * between reading that answer and acting on it.
         *
         * <p>
         * {@code concatMap} rather than {@code flatMap}: candidates must be tried one
         * at a time so the first success wins, instead of claiming several slots
         * concurrently and abandoning all but one.
         */
        private Mono<SlotClaim> allocateSlot(String parkingLotId) {
                return slotRepository.findAvailableSlotIds(parkingLotId, SLOT_CANDIDATES)
                                .concatMap(this::claimIfStillFree)
                                .next();
        }

        private Mono<SlotClaim> claimIfStillFree(Integer slotId) {
                return redisStateService.getSlotStatus(slotId)
                                // Redis has never seen this slot, so it cannot contradict
                                // PostgreSQL, which already said AVAILABLE.
                                .defaultIfEmpty(SLOT_AVAILABLE)
                                .filter(SLOT_AVAILABLE::equalsIgnoreCase)
                                .flatMap(status -> lockService.claimSlot(slotId))
                                .map(lock -> new SlotClaim(slotId, lock));
        }

        /**
         * Exit the vehicle behind a number plate - the camera-driven path.
         *
         * <p>
         * A camera exit names a car, not a session, so the open session is looked up
         * from the vehicle and handed to {@link #exitVehicle} unchanged.
         */
        public Mono<Void> exitVehicleByVehicleId(Integer vehicleId) {
                return sessionRepository.findActiveByVehicleId(vehicleId)
                                .switchIfEmpty(Mono.error(new IllegalStateException(
                                                "Cannot exit vehicle " + vehicleId + ": it holds no open session.")))
                                .flatMap(session -> exitVehicle(new ParkedVehicle(
                                                session.sessionId(),
                                                session.slotId(),
                                                // Unknown here by design; exitVehicle resolves the payment by
                                                // session, which is the same path an adopted session takes.
                                                null,
                                                session.userId(),
                                                session.vehicleId(),
                                                session.actualEntryTime())));
        }

        /**
         * Exit exactly one vehicle: the session moves CREATED -> EXITED, the slot back
         * to AVAILABLE, and the payment PENDING -> SUCCESS (the session's own
         * payment_status column spells the same transition PAID).
         *
         * <p>
         * The session version is read from Redis rather than assumed, and both the
         * expected and the new version travel on the event so the consumer can apply
         * the row update under an optimistic lock.
         *
         * <p>
         * Payment is published as a SEPARATE event (third-party) while session and slot
         * remain in the same lifecycle transaction.
         *
         * <p>
         * Randomly simulates payment failure (10% chance) to test error handling.
         */
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
                                                        SESSION_PAYMENT_PAID,
                                                        newVersion,
                                                        now);

                                        Integer slotId = session.slotId() == null ? parked.slotId() : session.slotId();

                                        // Resolve payment first (needed for separate publish)
                                        return resolvePaidPayment(parked, sessionId, now)
                                                        .switchIfEmpty(Mono.error(new IllegalStateException(
                                                                        "Cannot exit session " + sessionId
                                                                                        + ": no payment is known to Redis or to PostgreSQL.")))
                                                        .flatMap(payment -> {
                                                                // Randomly determine payment outcome:
                                                                // 30% PENDING, 10% FAILED, 60% SUCCESS
                                                                double r = RANDOM.nextDouble();
                                                                String paymentStatus;
                                                                String sessionPaymentStatus;
                                                                if (r < PAYMENT_PENDING_RATE) {
                                                                        paymentStatus = PAYMENT_PENDING;
                                                                        sessionPaymentStatus = PAYMENT_PENDING;
                                                                } else if (r < PAYMENT_PENDING_RATE + PAYMENT_FAILURE_RATE) {
                                                                        paymentStatus = PAYMENT_FAILED;
                                                                        sessionPaymentStatus = SESSION_PAYMENT_FAILED;
                                                                } else {
                                                                        paymentStatus = PAYMENT_SUCCESS;
                                                                        sessionPaymentStatus = SESSION_PAYMENT_PAID;
                                                                }

                                                                // Update payment status
                                                                PaymentEventPayload paidPayment = new PaymentEventPayload(
                                                                                payment.paymentId(),
                                                                                payment.transactionReference(),
                                                                                payment.sessionId(),
                                                                                payment.userId(),
                                                                                payment.methodId(),
                                                                                payment.amount(),
                                                                                payment.currency(),
                                                                                paymentStatus,
                                                                                payment.gatewayResponseCode(),
                                                                                payment.gatewayResponseMessage(),
                                                                                now,
                                                                                now);

                                                                // Update session payment status based on outcome
                                                                SessionEventPayload finalExited = new SessionEventPayload(
                                                                                exited.sessionId(),
                                                                                exited.reservationId(),
                                                                                exited.slotId(),
                                                                                exited.userId(),
                                                                                exited.vehicleId(),
                                                                                exited.entryGateId(),
                                                                                exited.exitGateId(),
                                                                                exited.sessionStatus(),
                                                                                exited.actualEntryTime(),
                                                                                exited.actualExitTime(),
                                                                                exited.expectedExitTime(),
                                                                                exited.durationMinutes(),
                                                                                sessionPaymentStatus,
                                                                                exited.sessionVersion(),
                                                                                exited.updatedAt());

                                                                // Save session and slot to Redis (lifecycle)
                                                                return currentSlot(slotId)
                                                                                .map(slot -> withStatus(slot,
                                                                                                SLOT_AVAILABLE, now))
                                                                                .switchIfEmpty(Mono.error(
                                                                                                new IllegalStateException(
                                                                                                                "Cannot exit session "
                                                                                                                                + sessionId
                                                                                                                                + ": slot "
                                                                                                                                + slotId
                                                                                                                                + " is unknown to Redis and to PostgreSQL.")))
                                                                                .flatMap(slot -> redisStateService
                                                                                                .saveSessionState(
                                                                                                                finalExited,
                                                                                                                newVersion)
                                                                                                .flatMap(storedSession -> redisStateService
                                                                                                                .saveSlotState(slot,
                                                                                                                                newVersion)
                                                                                                                .flatMap(storedSlot -> {
                                                                                                                        // Save
                                                                                                                        // payment
                                                                                                                        // to
                                                                                                                        // Redis
                                                                                                                        // separately
                                                                                                                        return redisStateService
                                                                                                                                        .savePaymentState(
                                                                                                                                                        paidPayment,
                                                                                                                                                        newVersion)
                                                                                                                                        .flatMap(storedPayment -> {
                                                                                                                                                // Publish
                                                                                                                                                // lifecycle
                                                                                                                                                // event
                                                                                                                                                // (session
                                                                                                                                                // +
                                                                                                                                                // slot)
                                                                                                                                                return eventPublisher
                                                                                                                                                                .publishVehicleExit(
                                                                                                                                                                                new ParkingLifecycleEventPayload(
                                                                                                                                                                                                storedSession,
                                                                                                                                                                                                storedSlot,
                                                                                                                                                                                                null, // payment
                                                                                                                                                                                                      // is
                                                                                                                                                                                                      // separate
                                                                                                                                                                                                expectedVersion,
                                                                                                                                                                                                newVersion),
                                                                                                                                                                                newVersion)
                                                                                                                                                                .then(
                                                                                                                                                                                // Publish
                                                                                                                                                                                // payment
                                                                                                                                                                                // event
                                                                                                                                                                                // separately
                                                                                                                                                                                // (third-party)
                                                                                                                                                                                eventPublisher.publishPaymentCompleted(
                                                                                                                                                                                                storedPayment,
                                                                                                                                                                                                newVersion))
                                                                                                                                                                .doOnSuccess(ignored -> LOGGER
                                                                                                                                                                                .info(
                                                                                                                                                                                                "EXIT | session={} slot={} payment={} status={} version {}->{} written to Redis and published (payment separate)",
                                                                                                                                                                                                sessionId,
                                                                                                                                                                                                slotId,
                                                                                                                                                                                                storedPayment.paymentId(),
                                                                                                                                                                                                paymentStatus,
                                                                                                                                                                                                expectedVersion,
                                                                                                                                                                                                newVersion));
                                                                                                                                        });
                                                                                                                })));
                                                        });
                                })
                                .then();
        }

        /**
         * The payment to settle. Sessions the simulation adopted from a
         * previous run carry no known payment id, so those fall back to a lookup by
         * session.
         *
         * <p>
         * Completes empty when neither store knows of a payment, and the caller turns
         * that into an error. Minting one here would not help: the consumer settles an
         * exit with an UPDATE, so an invented payment would match no row, report zero
         * rows and dead letter the event anyway. Keeping the exit loop alive is the
         * caller's job, not this method's - SimulationService retries a failed exit and
         * abandons the session after {@code MAX_EXIT_ATTEMPTS}.
         *
         * <p>
         * The returned payment carries the status from the store (PENDING from Redis/PostgreSQL).
         * The actual status transition to SUCCESS/FAILED is applied in {@link #exitVehicle}
         * based on the random failure rate.
         */
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

        /**
         * Session as the real-time store sees it, falling back to PostgreSQL.
         *
         * <p>
         * The fallback is what lets an exit drain a session left open by a previous
         * run. SimulationService#adoptExistingSessions deliberately takes those over
         * from PostgreSQL so their slots are not stranded, but they were never written
         * to Redis, so reading Redis alone rejected exactly the sessions adoption
         * exists to rescue. Each one then failed MAX_EXIT_ATTEMPTS times, was
         * abandoned, and freed its user - whereupon entry re-parked the same vehicle
         * and hit unique_active_session_per_vehicle, dead lettering every future entry
         * for that vehicle. One unexitable session permanently poisoned one vehicle.
         */
        private Mono<SessionEventPayload> currentSession(Integer sessionId) {
                return redisStateService.getSession(sessionId)
                                .switchIfEmpty(sessionRepository.findById(sessionId)
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
                                                                session.sessionVersion(),
                                                                session.updatedAt())));
        }

        /**
         * Slot as the real-time store sees it, falling back to PostgreSQL the first
         * time a slot is touched after startup so locationId, displayCode and sensorId
         * are never replaced with nulls.
         */
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

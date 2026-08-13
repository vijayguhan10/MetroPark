package com.example.Metropark.parking.repo;

import java.time.LocalDateTime;

import org.jooq.DSLContext;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Repository;

import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;
import com.example.Metropark.event.payload.SessionEventPayload;
import com.example.Metropark.event.payload.SlotEventPayload;

import reactor.core.publisher.Mono;

/**
 * Persists one consumed vehicle-entry or vehicle-exit event to PostgreSQL.
 *
 * <p>
 * For ENTRY: All writes run inside {@code dsl.transactionPublisher}, so
 * parking_sessions, parking_slots and payments commit together or not at all.
 *
 * <p>
 * For EXIT: Only parking_sessions and parking_slots commit together in one
 * transaction.
 * Payment is handled separately as a third-party event by the payment consumer.
 *
 * <p>
 * This is deliberate rather than {@code @Transactional}: the DSLContext is
 * built
 * from the raw R2DBC ConnectionFactory
 * ({@code DSL.using(connectionFactory, POSTGRES)}), so it acquires its own
 * connection per query and never joins the transaction Spring's reactive
 * transaction manager binds to the subscriber context. {@code @Transactional}
 * on
 * these paths is silently a no-op; jOOQ's own transaction publisher is not.
 */
@Repository
public class ParkingLifecycleRepository {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParkingLifecycleRepository.class);

        private final DSLContext dsl;

        public ParkingLifecycleRepository(DSLContext dsl) {
                this.dsl = dsl;
        }

        /**
         * Row counts for each table touched while persisting one lifecycle event.
         */
        public record PersistResult(int sessionRows, int slotRows, int paymentRows) {
        }

        /**
         * Vehicle entry: insert the session, mark the slot OCCUPIED, insert the
         * pending payment - in that order, inside one transaction.
         *
         * <p>
         * Both inserts are ON CONFLICT DO NOTHING so a RabbitMQ redelivery (which is
         * always possible once ACK is manual) re-applies cleanly instead of failing on
         * a duplicate key and looping into the dead letter queue.
         */
        public Mono<PersistResult> persistEntry(ParkingLifecycleEventPayload payload) {
                SessionEventPayload session = payload.session();
                SlotEventPayload slot = payload.slot();
                PaymentEventPayload payment = payload.payment();

                return Mono.from(dsl.transactionPublisher(configuration -> {
                        DSLContext tx = DSL.using(configuration);

                        Mono<Integer> insertSession = Mono.from(tx.insertInto(table("parking_sessions"))
                                        .columns(
                                                        field("session_id"),
                                                        field("reservation_id"),
                                                        field("slot_id"),
                                                        field("user_id"),
                                                        field("vehicle_id"),
                                                        field("entry_gate_id"),
                                                        field("exit_gate_id"),
                                                        field("session_status"),
                                                        field("actual_entry_time"),
                                                        field("actual_exit_time"),
                                                        field("expected_exit_time"),
                                                        field("duration_minutes"),
                                                        field("payment_status"),
                                                        field("surge_multiplier"),
                                                        field("session_version"),
                                                        field("created_at"),
                                                        field("updated_at"))
                                        .values(
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
                                                        session.surgeMultiplier() != null ? session.surgeMultiplier() : java.math.BigDecimal.ONE,
                                                        session.sessionVersion(),
                                                        session.updatedAt(),
                                                        session.updatedAt())
                                        // Targeted at the primary key, NOT a blanket
                                        // onDuplicateKeyIgnore(). parking_sessions also carries the
                                        // partial unique index unique_active_session_per_vehicle;
                                        // an untargeted ON CONFLICT DO NOTHING would swallow that
                                        // violation too, skip the row, and leave the payment insert
                                        // below to fail on its session_id foreign key - the same
                                        // dead letter, several statements away from its cause.
                                        .onConflict(field("session_id")).doNothing())
                                        .defaultIfEmpty(0)
                                        .doOnNext(rows -> LOGGER.info(
                                                        "POSTGRES session INSERT | session={} status={} version={} rowsAffected={}",
                                                        session.sessionId(), session.sessionStatus(),
                                                        session.sessionVersion(), rows));

                        Mono<Integer> updateSlot = slot == null
                                        ? Mono.just(0)
                                        : Mono.from(tx.update(table("parking_slots"))
                                                        .set(field("current_status"), slot.currentStatus())
                                                        .set(field("updated_at"), slot.updatedAt())
                                                        .where(field("slot_id").eq(slot.slotId())))
                                                        .defaultIfEmpty(0)
                                                        .doOnNext(rows -> LOGGER.info(
                                                                        "POSTGRES slot UPDATE | slot={} status={} rowsAffected={}",
                                                                        slot.slotId(), slot.currentStatus(), rows));

                        Mono<Integer> insertPayment = payment == null
                                        ? Mono.just(0)
                                        : Mono.from(tx.insertInto(table("payments"))
                                                        .columns(
                                                                        field("payment_id"),
                                                                        field("transaction_reference"),
                                                                        field("session_id"),
                                                                        field("user_id"),
                                                                        field("method_id"),
                                                                        field("amount"),
                                                                        field("currency"),
                                                                        field("payment_status"),
                                                                        field("gateway_response_code"),
                                                                        field("gateway_response_message"),
                                                                        field("processed_at"),
                                                                        field("created_at"),
                                                                        field("updated_at"))
                                                        .values(
                                                                        payment.paymentId(),
                                                                        payment.transactionReference(),
                                                                        payment.sessionId(),
                                                                        payment.userId(),
                                                                        payment.methodId(),
                                                                        payment.amount(),
                                                                        payment.currency(),
                                                                        payment.paymentStatus(),
                                                                        payment.gatewayResponseCode(),
                                                                        payment.gatewayResponseMessage(),
                                                                        payment.processedAt(),
                                                                        payment.updatedAt(),
                                                                        payment.updatedAt())
                                                        .onConflict(field("payment_id")).doNothing())
                                                        .defaultIfEmpty(0)
                                                        .doOnNext(rows -> LOGGER.info(
                                                                        "POSTGRES payment INSERT | payment={} session={} status={} rowsAffected={}",
                                                                        payment.paymentId(), payment.sessionId(),
                                                                        payment.paymentStatus(), rows));

                        // concatMap semantics via sequential composition: the slot write is
                        // only issued once the session write has completed, and the payment
                        // write once the slot write has completed.
                        return insertSession.flatMap(sessionRows -> updateSlot
                                        .flatMap(slotRows -> insertPayment
                                                        .map(paymentRows -> new PersistResult(sessionRows, slotRows,
                                                                        paymentRows))));
                }));
        }

        /**
         * Vehicle exit, in the mandated order: parking_sessions (EXITED) then
         * parking_slots (AVAILABLE), inside one transaction.
         * Payment is handled separately as a third-party event.
         *
         * <p>
         * The session update is guarded by an optimistic lock on
         * {@code session_version}. A zero row count is escalated as an error so the
         * message is dead lettered instead of being acknowledged as if it had been
         * applied - that silent-success path is what let Redis and PostgreSQL diverge.
         */
        public Mono<PersistResult> persistExit(ParkingLifecycleEventPayload payload) {
                SessionEventPayload session = payload.session();
                SlotEventPayload slot = payload.slot();

                Integer expectedVersion = payload.expectedSessionVersion();
                Integer newVersion = payload.newSessionVersion();

                return Mono.from(dsl.transactionPublisher(configuration -> {
                        DSLContext tx = DSL.using(configuration);

                        Mono<Integer> updateSession = Mono.from(tx.update(table("parking_sessions"))
                                        .set(field("session_status"), session.sessionStatus())
                                        .set(field("payment_status"), session.paymentStatus())
                                        .set(field("actual_exit_time"), session.actualExitTime())
                                        .set(field("duration_minutes"), session.durationMinutes())
                                        .set(field("session_version"), newVersion)
                                        .set(field("updated_at"), LocalDateTime.now())
                                        .where(field("session_id").eq(session.sessionId()))
                                        // `in(expected, new)` keeps the update idempotent: a redelivered
                                        // message finds the row already at newVersion and still matches.
                                        .and(field("session_version").in(expectedVersion, newVersion)))
                                        .defaultIfEmpty(0)
                                        .doOnNext(rows -> LOGGER.info(
                                                        "POSTGRES session UPDATE | session={} status={} expectedVersion={} newVersion={} rowsAffected={}",
                                                        session.sessionId(), session.sessionStatus(), expectedVersion,
                                                        newVersion, rows))
                                        .flatMap(rows -> rows > 0
                                                        ? Mono.just(rows)
                                                        : Mono.error(new IllegalStateException(
                                                                        "parking_sessions update affected 0 rows for session "
                                                                                        + session.sessionId()
                                                                                        + " (expected session_version "
                                                                                        + expectedVersion + " or "
                                                                                        + newVersion + ")")));

                        Mono<Integer> updateSlot = slot == null
                                        ? Mono.just(0)
                                        : Mono.from(tx.update(table("parking_slots"))
                                                        .set(field("current_status"), slot.currentStatus())
                                                        .set(field("updated_at"), slot.updatedAt())
                                                        .where(field("slot_id").eq(slot.slotId())))
                                                        .defaultIfEmpty(0)
                                                        .doOnNext(rows -> LOGGER.info(
                                                                        "POSTGRES slot UPDATE | slot={} status={} rowsAffected={}",
                                                                        slot.slotId(), slot.currentStatus(), rows))
                                                        .flatMap(rows -> rows > 0
                                                                        ? Mono.just(rows)
                                                                        : Mono.error(new IllegalStateException(
                                                                                        "parking_slots update affected 0 rows for slot "
                                                                                                        + slot.slotId())));

                        // Payment is NOT updated here - it's a separate third-party event
                        // handled by the payment consumer

                        return updateSession.flatMap(sessionRows -> updateSlot
                                        .map(slotRows -> new PersistResult(sessionRows, slotRows, 0)));
                }));
        }
}

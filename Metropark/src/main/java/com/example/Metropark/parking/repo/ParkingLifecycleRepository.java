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

@Repository
public class ParkingLifecycleRepository {

        private static final Logger LOGGER = LoggerFactory.getLogger(ParkingLifecycleRepository.class);

        private final DSLContext dsl;

        public ParkingLifecycleRepository(DSLContext dsl) {
                this.dsl = dsl;
        }

        public record PersistResult(int sessionRows, int slotRows, int paymentRows) {
        }

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
                                                        session.surgeMultiplier() != null ? session.surgeMultiplier()
                                                                        : java.math.BigDecimal.ONE,
                                                        session.sessionVersion(),
                                                        session.updatedAt(),
                                                        session.updatedAt())
                                        .onConflict(field("session_id")).doNothing())
                                        .defaultIfEmpty(0)
                                        .onErrorResume(error -> {
                                                // A re-entry for a vehicle that still holds a CREATED/ACTIVE
                                                // session trips the unique_active_session_per_vehicle
                                                // constraint. Treat this as idempotent (already persisted)
                                                // so the message is acknowledged instead of dead-lettered.
                                                if (isUniqueViolation(error)) {
                                                        LOGGER.info(
                                                                        "POSTGRES session INSERT skipped | session={} vehicle={} already has an active session (unique constraint)",
                                                                        session.sessionId(), session.vehicleId());
                                                        return Mono.just(0);
                                                }
                                                return Mono.error(error);
                                        })
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

                        return insertSession.flatMap(sessionRows -> updateSlot
                                        .flatMap(slotRows -> insertPayment
                                                        .map(paymentRows -> new PersistResult(sessionRows, slotRows,
                                                                        paymentRows))));
                }));
        }

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

                        return updateSession.flatMap(sessionRows -> updateSlot
                                        .map(slotRows -> new PersistResult(sessionRows, slotRows, 0)));
                }));
        }

        private static boolean isUniqueViolation(Throwable error) {
                Throwable current = error;
                while (current != null) {
                        if (current instanceof org.jooq.exception.DataAccessException dae
                                        && dae.sqlStateClass() == org.jooq.exception.SQLStateClass.C23_INTEGRITY_CONSTRAINT_VIOLATION) {
                                String sqlState = dae.sqlState();
                                if ("23505".equals(sqlState)) {
                                        return true;
                                }
                        }
                        if (current.getClass().getName().startsWith("org.postgresql.util.PSQLException")
                                        || current.getClass().getName()
                                                        .startsWith("org.postgresql.util.ServerErrorMessage")) {
                                String message = current.getMessage();
                                if (message != null && message.contains("unique_active_session_per_vehicle")) {
                                        return true;
                                }
                        }
                        current = current.getCause();
                }
                return false;
        }
}

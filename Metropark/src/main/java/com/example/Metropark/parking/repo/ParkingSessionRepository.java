package com.example.Metropark.parking.repo;

import java.time.LocalDateTime;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSessionResponseDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class ParkingSessionRepository {

        private final DSLContext dsl;

        public ParkingSessionRepository(DSLContext dsl) {
                this.dsl = dsl;
        }

        /**
         * Reserves the primary key from the PostgreSQL identity sequence WITHOUT
         * writing any row. Redis-first requires an id before the session exists in
         * PostgreSQL; taking it from the real sequence guarantees the id Redis
         * publishes is the same id the consumer inserts, and that it can never
         * collide with a row inserted through the ordinary auto-increment path.
         */
        public Mono<Integer> allocateSessionId() {
                return Mono.from(dsl.select(
                                field("nextval(pg_get_serial_sequence('parking_sessions', 'session_id'))",
                                                Long.class)))
                                .map(record -> record.get(0, Long.class).intValue());
        }

        public Mono<Integer> create(ParkingSessionDto dto) {
                return create(dto, null);
        }

        public Mono<Integer> create(ParkingSessionDto dto, Integer explicitId) {
                if (explicitId != null) {
                        return Mono.from(
                                        dsl.insertInto(table("parking_sessions"))
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
                                                                        field("session_version"),
                                                                        field("created_at"),
                                                                        field("updated_at"))
                                                        .values(
                                                                        explicitId,
                                                                        dto.reservationId(),
                                                                        dto.slotId(),
                                                                        dto.userId(),
                                                                        dto.vehicleId(),
                                                                        dto.entryGateId(),
                                                                        dto.exitGateId(),
                                                                        dto.sessionStatus(),
                                                                        dto.actualEntryTime(),
                                                                        dto.actualExitTime(),
                                                                        dto.expectedExitTime(),
                                                                        dto.durationMinutes(),
                                                                        dto.paymentStatus(),
                                                                        dto.sessionVersion(),
                                                                        dto.createdAt(),
                                                                        dto.updatedAt())
                                                        .returning(field("session_id")))
                                        .map(record -> (Integer) record.get(field("session_id")));
                }

                return Mono.from(
                                dsl.insertInto(table("parking_sessions"))
                                                .columns(
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
                                                                field("session_version"),
                                                                field("created_at"),
                                                                field("updated_at"))
                                                .values(
                                                                dto.reservationId(),
                                                                dto.slotId(),
                                                                dto.userId(),
                                                                dto.vehicleId(),
                                                                dto.entryGateId(),
                                                                dto.exitGateId(),
                                                                dto.sessionStatus(),
                                                                dto.actualEntryTime(),
                                                                dto.actualExitTime(),
                                                                dto.expectedExitTime(),
                                                                dto.durationMinutes(),
                                                                dto.paymentStatus(),
                                                                dto.sessionVersion(),
                                                                dto.createdAt(),
                                                                dto.updatedAt())
                                                .returning(field("session_id")))
                                .map(record -> (Integer) record.get(field("session_id")));
        }

        public Flux<ParkingSessionDto> findAll() {
                return Flux.from(dsl.selectFrom(table("parking_sessions"))).map(this::mapToDto);
        }

        public Mono<ParkingSessionDto> findById(Integer id) {
                return Mono.from(dsl.selectFrom(table("parking_sessions"))
                                .where(field("session_id").eq(id)))
                                .map(this::mapToDto);
        }

        public Flux<ParkingSessionResponseDto> findAllWithDetails() {
                var query = dsl.select(

                                field("ps.session_id").as("sessionId"),
                                field("ps.reservation_id").as("reservationId"),
                                field("ps.slot_id").as("slotId"),
                                field("psl.display_code").as("slotDisplayCode"),
                                field("psl.location_id").as("locationId"),
                                field("loc.location_name").as("locationName"),
                                field("ps.user_id").as("userId"),
                                field("u.name").as("userName"),
                                field("ps.vehicle_id").as("vehicleId"),
                                field("v.vehicle_number").as("vehicleNumber"),
                                field("ps.entry_gate_id").as("entryGateId"),
                                field("eg.gate_name").as("entryGateName"),
                                field("ps.exit_gate_id").as("exitGateId"),
                                field("xg.gate_name").as("exitGateName"),
                                field("ps.session_status").as("sessionStatus"),
                                field("ps.actual_entry_time").as("actualEntryTime"),
                                field("ps.actual_exit_time").as("actualExitTime"),
                                field("ps.expected_exit_time").as("expectedExitTime"),
                                field("ps.duration_minutes").as("durationMinutes"),
                                field("ps.payment_status").as("paymentStatus"),
                                field("ps.session_version").as("sessionVersion"),
                                field("ps.created_at").as("createdAt"),
                                field("ps.updated_at").as("updatedAt"))
                                .from(table("parking_sessions").as("ps"))
                                .join(table("parking_slots").as("psl")).on(field("ps.slot_id").eq(field("psl.slot_id")))
                                .join(table("locations").as("loc"))
                                .on(field("psl.location_id").eq(field("loc.location_id")))
                                .join(table("users").as("u")).on(field("ps.user_id").eq(field("u.user_id")))
                                .join(table("vehicles").as("v")).on(field("ps.vehicle_id").eq(field("v.vehicle_id")))
                                .leftJoin(table("gates").as("eg")).on(field("ps.entry_gate_id").eq(field("eg.gate_id")))
                                .leftJoin(table("gates").as("xg")).on(field("ps.exit_gate_id").eq(field("xg.gate_id")));

                return Flux.from(query)
                                .map(record -> {

                                        return record.into(ParkingSessionResponseDto.class);
                                });
        }

        public Mono<ParkingSessionResponseDto> findByIdWithDetails(Integer sessionId) {
                var query = dsl.select(

                                field("ps.session_id").as("sessionId"),
                                field("ps.reservation_id").as("reservationId"),
                                field("ps.slot_id").as("slotId"),
                                field("psl.display_code").as("slotDisplayCode"),
                                field("psl.location_id").as("locationId"),
                                field("loc.location_name").as("locationName"),
                                field("ps.user_id").as("userId"),
                                field("u.name").as("userName"),
                                field("ps.vehicle_id").as("vehicleId"),
                                field("v.vehicle_number").as("vehicleNumber"),
                                field("ps.entry_gate_id").as("entryGateId"),
                                field("eg.gate_name").as("entryGateName"),
                                field("ps.exit_gate_id").as("exitGateId"),
                                field("xg.gate_name").as("exitGateName"),
                                field("ps.session_status").as("sessionStatus"),
                                field("ps.actual_entry_time").as("actualEntryTime"),
                                field("ps.actual_exit_time").as("actualExitTime"),
                                field("ps.expected_exit_time").as("expectedExitTime"),
                                field("ps.duration_minutes").as("durationMinutes"),
                                field("ps.payment_status").as("paymentStatus"),
                                field("ps.session_version").as("sessionVersion"),
                                field("ps.created_at").as("createdAt"),
                                field("ps.updated_at").as("updatedAt"))
                                .from(table("parking_sessions").as("ps"))
                                .join(table("parking_slots").as("psl")).on(field("ps.slot_id").eq(field("psl.slot_id")))
                                .join(table("locations").as("loc"))
                                .on(field("psl.location_id").eq(field("loc.location_id")))
                                .join(table("users").as("u")).on(field("ps.user_id").eq(field("u.user_id")))
                                .join(table("vehicles").as("v")).on(field("ps.vehicle_id").eq(field("v.vehicle_id")))
                                .leftJoin(table("gates").as("eg")).on(field("ps.entry_gate_id").eq(field("eg.gate_id")))
                                .leftJoin(table("gates").as("xg")).on(field("ps.exit_gate_id").eq(field("xg.gate_id")))
                                .where(field("ps.session_id").eq(sessionId));

                return Mono.from(query)
                                .map(record -> record.into(ParkingSessionResponseDto.class));
        }

        /**
         * {@code defaultIfEmpty(0)} matters: when the WHERE clause matches nothing the
         * reactive jOOQ publisher can complete without emitting, and a bare
         * {@code flatMap} downstream would then be skipped entirely - the caller sees
         * an empty Mono and concludes "success" for an update that affected no rows.
         */
        public Mono<Integer> updateStatusWithOptimisticLock(Integer id, String status, Integer currentVersion) {
                return Mono.from(dsl.update(table("parking_sessions"))
                                .set(field("session_status"), status)
                                .set(field("updated_at"), LocalDateTime.now())
                                .set(field("session_version"), currentVersion + 1)
                                .where(field("session_id").eq(id))
                                .and(field("session_version").eq(currentVersion)))
                                .defaultIfEmpty(0);
        }

        /**
         * Applies the full exit transition carried by a consumed event under an
         * optimistic lock on {@code session_version}. Idempotent on redelivery: if the
         * row already carries {@code newVersion} the second predicate matches and the
         * update is a harmless no-op that still reports a row.
         */
        public Mono<Integer> applyExitWithOptimisticLock(
                        Integer id,
                        String status,
                        String paymentStatus,
                        LocalDateTime actualExitTime,
                        Integer durationMinutes,
                        Integer expectedVersion,
                        Integer newVersion) {

                return Mono.from(dsl.update(table("parking_sessions"))
                                .set(field("session_status"), status)
                                .set(field("payment_status"), paymentStatus)
                                .set(field("actual_exit_time"), actualExitTime)
                                .set(field("duration_minutes"), durationMinutes)
                                .set(field("session_version"), newVersion)
                                .set(field("updated_at"), LocalDateTime.now())
                                .where(field("session_id").eq(id))
                                .and(field("session_version").in(expectedVersion, newVersion)))
                                .defaultIfEmpty(0);
        }

        public Mono<Boolean> existsById(Integer id) {
                return Mono.from(dsl.selectOne()
                                .from(table("parking_sessions"))
                                .where(field("session_id").eq(id)))
                                .map(record -> true)
                                .defaultIfEmpty(false);
        }

        /**
         * The open session for a vehicle, if it has one.
         *
         * <p>
         * This is how a camera exit finds what to close: the event carries a number
         * plate, never a session id, so the session has to be looked up from the
         * vehicle behind that plate. {@code unique_active_session_per_vehicle}
         * guarantees at most one row matches CREATED or ACTIVE, so the newest-first
         * ordering only matters for the RESERVED case.
         */
        public Mono<ParkingSessionDto> findActiveByVehicleId(Integer vehicleId) {
                return Mono.from(dsl.selectFrom(table("parking_sessions"))
                                .where(field("vehicle_id").eq(vehicleId))
                                .and(field("session_status").in("RESERVED", "CREATED", "ACTIVE"))
                                .orderBy(field("session_id").desc())
                                .limit(1))
                                .map(this::mapToDto);
        }

        public Mono<Boolean> hasActiveSession(Integer vehicleId) {
                return Mono.from(dsl.selectFrom(table("parking_sessions"))
                                .where(field("vehicle_id").eq(vehicleId))
                                .and(field("session_status").in("RESERVED", "CREATED", "ACTIVE"))
                                .limit(1))
                                .map(record -> true).defaultIfEmpty(false);
        }

        private ParkingSessionDto mapToDto(Record record) {
                return new ParkingSessionDto(
                                record.get("session_id", Integer.class),
                                record.get("reservation_id", Integer.class),
                                record.get("slot_id", Integer.class),
                                record.get("user_id", String.class),
                                record.get("vehicle_id", Integer.class),
                                record.get("entry_gate_id", Integer.class),
                                record.get("exit_gate_id", Integer.class),
                                record.get("session_status", String.class),
                                record.get("actual_entry_time", LocalDateTime.class),
                                record.get("actual_exit_time", LocalDateTime.class),
                                record.get("expected_exit_time", LocalDateTime.class),
                                record.get("duration_minutes", Integer.class),
                                record.get("payment_status", String.class),
                                record.get("session_version", Integer.class),
                                record.get("created_at", LocalDateTime.class),
                                record.get("updated_at", LocalDateTime.class));
        }
}
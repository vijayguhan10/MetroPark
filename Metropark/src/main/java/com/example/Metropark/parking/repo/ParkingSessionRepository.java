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

        public Mono<Integer> create(ParkingSessionDto dto) {
                return Mono.from(dsl.insertInto(table("parking_sessions"))
                                .columns(
                                                field("reservation_id"), field("slot_id"), field("user_id"),
                                                field("vehicle_id"), field("entry_gate_id"), field("exit_gate_id"),
                                                field("session_status"), field("actual_entry_time"),
                                                field("actual_exit_time"),
                                                field("expected_exit_time"), field("duration_minutes"),
                                                field("payment_status"),
                                                field("session_version"), field("created_at"), field("updated_at"))
                                .values(
                                                dto.reservationId(), dto.slotId(), dto.userId(),
                                                dto.vehicleId(), dto.entryGateId(), dto.exitGateId(),
                                                dto.sessionStatus(), dto.actualEntryTime(), dto.actualExitTime(),
                                                dto.expectedExitTime(), dto.durationMinutes(), dto.paymentStatus(),
                                                dto.sessionVersion(), dto.createdAt(), dto.updatedAt()));
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
                                // ORDER MUST MATCH THE RECORD DTO EXACTLY
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
                                .join(table("locations").as("loc")).on(field("psl.location_id").eq(field("loc.location_id")))
                                .join(table("users").as("u")).on(field("ps.user_id").eq(field("u.user_id")))
                                .join(table("vehicles").as("v")).on(field("ps.vehicle_id").eq(field("v.vehicle_id")))
                                .leftJoin(table("gates").as("eg")).on(field("ps.entry_gate_id").eq(field("eg.gate_id")))
                                .leftJoin(table("gates").as("xg")).on(field("ps.exit_gate_id").eq(field("xg.gate_id")));

                return Flux.from(query)
                                .map(record -> record.into(ParkingSessionResponseDto.class));
        }

        public Mono<ParkingSessionResponseDto> findByIdWithDetails(Integer sessionId) {
                var query = dsl.select(
                                // ORDER MUST MATCH THE RECORD DTO EXACTLY
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
                                .join(table("locations").as("loc")).on(field("psl.location_id").eq(field("loc.location_id")))
                                .join(table("users").as("u")).on(field("ps.user_id").eq(field("u.user_id")))
                                .join(table("vehicles").as("v")).on(field("ps.vehicle_id").eq(field("v.vehicle_id")))
                                .leftJoin(table("gates").as("eg")).on(field("ps.entry_gate_id").eq(field("eg.gate_id")))
                                .leftJoin(table("gates").as("xg")).on(field("ps.exit_gate_id").eq(field("xg.gate_id")))
                                .where(field("ps.session_id").eq(sessionId));

                return Mono.from(query)
                                .map(record -> record.into(ParkingSessionResponseDto.class));
        }

        public Mono<Integer> updateStatusWithOptimisticLock(Integer id, String status, Integer currentVersion) {
                return Mono.from(dsl.update(table("parking_sessions"))
                                .set(field("session_status"), status)
                                .set(field("updated_at"), LocalDateTime.now())
                                .set(field("session_version"), currentVersion + 1)
                                .where(field("session_id").eq(id))
                                .and(field("session_version").eq(currentVersion)));
        }

        public Mono<Boolean> hasActiveSession(Integer vehicleId) {
                return Mono.from(dsl.selectFrom(table("parking_sessions"))
                                .where(field("vehicle_id").eq(vehicleId))
                                .and(field("session_status").in("CREATED", "ACTIVE"))
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
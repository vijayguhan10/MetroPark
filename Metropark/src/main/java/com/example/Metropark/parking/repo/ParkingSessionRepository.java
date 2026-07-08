package com.example.Metropark.parking.repo;

import com.example.Metropark.parking.dto.ParkingSessionDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

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
                        field("session_status"), field("actual_entry_time"), field("actual_exit_time"),
                        field("expected_exit_time"), field("duration_minutes"), field("payment_status"),
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
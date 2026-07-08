package com.example.Metropark.reservation.repo;

import com.example.Metropark.reservation.dto.ReservationDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class ReservationRepository {

    private final DSLContext dsl;

    public ReservationRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(ReservationDto dto) {
        return Mono.from(dsl.insertInto(table("reservations"))
                .columns(
                        field("user_id"), field("slot_id"), field("queue_entry_id"),
                        field("reservation_status"), field("reservation_version"),
                        field("reserved_at"), field("expires_at"),
                        field("created_at"), field("updated_at"))
                .values(
                        dto.userId(), dto.slotId(), dto.queueEntryId(),
                        dto.reservationStatus(), dto.reservationVersion(),
                        dto.reservedAt(), dto.expiresAt(),
                        dto.createdAt(), dto.updatedAt()));
    }

    public Flux<ReservationDto> findAll() {
        return Flux.from(dsl.selectFrom(table("reservations"))).map(this::mapToDto);
    }

    public Mono<ReservationDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("reservations"))
                .where(field("reservation_id").eq(id)))
                .map(this::mapToDto);
    }

    // THE OPTIMISTIC LOCKING QUERY
    public Mono<Integer> updateStatusWithOptimisticLock(
            Integer reservationId,
            String reservationStatus,
            Integer currentVersion) {

        return Mono.from(

                dsl.update(table("reservations"))
                        .set(field("reservation_status"), reservationStatus)
                        .set(field("reservation_version"), currentVersion + 1)
                        .set(field("updated_at"), LocalDateTime.now())
                        .where(field("reservation_id").eq(reservationId))
                        .and(field("reservation_version").eq(currentVersion))

        );
    }

    private ReservationDto mapToDto(Record record) {
        return new ReservationDto(
                record.get("reservation_id", Integer.class),
                record.get("user_id", Integer.class),
                record.get("slot_id", Integer.class),
                record.get("queue_entry_id", Integer.class),
                record.get("reservation_status", String.class),
                record.get("reservation_version", Integer.class),
                record.get("reserved_at", LocalDateTime.class),
                record.get("expires_at", LocalDateTime.class),
                record.get("created_at", LocalDateTime.class),
                record.get("updated_at", LocalDateTime.class));
    }
}
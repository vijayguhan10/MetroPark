package com.example.Metropark.parking.repo;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.example.Metropark.parking.dto.ParkingSlotDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class ParkingSlotRepository {

    private final DSLContext dsl;

    public ParkingSlotRepository(DSLContext dsl) {
        this.dsl = dsl;
    }
    @Transactional
    public Mono<Integer> create(ParkingSlotDto dto) {

        return Mono.from(

                dsl.insertInto(table("parking_slots"))
                        .columns(
                                field("location_id"),
                                field("display_code"),
                                field("vehicle_type_id"),
                                field("reservation_class_id"),
                                field("sensor_id"),
                                field("current_status")
                        )
                        .values(
                                dto.locationId(),
                                dto.displayCode(),
                                dto.vehicleTypeId(),
                                dto.reservationClassId(),
                                dto.sensorId(),
                                dto.currentStatus()
                        )
                        .returning(field("slot_id"))
        )
        .map(record -> (Integer) record.get(field("slot_id")));
    }

    public Flux<ParkingSlotDto> findAll() {

        return Flux.from(

                dsl.selectFrom(table("parking_slots"))

        ).map(this::mapToDto);
    }

    public Mono<ParkingSlotDto> findById(Integer id) {

        return Mono.from(

                dsl.selectFrom(table("parking_slots"))
                        .where(field("slot_id").eq(id))

        ).map(this::mapToDto);
    }

    /**
     * Candidate slots for an entry, cheapest-first by id.
     *
     * <p>
     * PostgreSQL is only a CANDIDATE source, never the decision: it still shows the
     * pre-entry status until the lifecycle consumer catches up, so a slot listed
     * here may already be occupied. The caller re-checks each candidate against
     * Redis and claims it atomically before using it. {@code limit} keeps that
     * re-check bounded rather than scanning the whole lot on every entry.
     */
    public Flux<Integer> findAvailableSlotIds(String locationId, int limit) {

        var condition = field("current_status").eq("AVAILABLE");

        return Flux.from(
                dsl.select(field("slot_id"))
                        .from(table("parking_slots"))
                        .where(locationId == null
                                ? condition
                                : condition.and(field("location_id").eq(locationId)))
                        .orderBy(field("slot_id"))
                        .limit(limit))
                .map(record -> record.get(field("slot_id"), Integer.class));
    }

    
    public Mono<Integer> reserveSlot(Integer slotId) {

        return Mono.from(

                dsl.update(table("parking_slots"))
                        .set(field("current_status"), "RESERVED")
                        .where(field("slot_id").eq(slotId))
                       

        );
    }

    /**
     * {@code defaultIfEmpty(0)} guarantees a row count is always emitted. Without it
     * an update that matches no row completes empty, every downstream flatMap is
     * skipped, and the caller's {@code then(...)} continues as if the slot had been
     * released.
     */
    public Mono<Integer> updateStatus(Integer slotId, String status) {

        return Mono.from(

                dsl.update(table("parking_slots"))
                        .set(field("current_status"), status)
                        .where(field("slot_id").eq(slotId))

        ).defaultIfEmpty(0);
    }

    private ParkingSlotDto mapToDto(Record record) {

        return new ParkingSlotDto(
                record.get("slot_id", Integer.class),
                record.get("location_id", String.class),
                record.get("display_code", String.class),
                record.get("vehicle_type_id", Integer.class),
                record.get("reservation_class_id", Integer.class),
                record.get("sensor_id", String.class),
                record.get("current_status", String.class)
        );
    }
}
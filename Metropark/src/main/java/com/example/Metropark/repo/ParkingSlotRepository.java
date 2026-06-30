package com.example.Metropark.repo;

import com.example.Metropark.dto.ParkingSlotDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class ParkingSlotRepository {

    private final DSLContext dsl;

    public ParkingSlotRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(ParkingSlotDto dto) {
        return Mono.from(dsl.insertInto(table("parking_slots"))
                .columns(
                        field("location_id"), field("display_code"), field("vehicle_type_id"),
                        field("reservation_class_id"), field("sensor_id"), field("current_status")
                )
                .values(
                        dto.locationId(), dto.displayCode(), dto.vehicleTypeId(),
                        dto.reservationClassId(), dto.sensorId(), dto.currentStatus()
                        
                ));
    }

    public Flux<ParkingSlotDto> findAll() {
        return Flux.from(dsl.selectFrom(table("parking_slots"))).map(this::mapToDto);
    }

    public Mono<ParkingSlotDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("parking_slots"))
                .where(field("slot_id").eq(id)))
                .map(this::mapToDto);
    }

    // THE OPTIMISTIC LOCKING QUERY
    // public Mono<Integer> updateStatusWithOptimisticLock(Integer id, String status, Integer currentVersion) {
    //     return Mono.from(dsl.update(table("parking_slots"))
    //             .set(field("current_status"), status)
    //             .set(field("updated_at"), LocalDateTime.now())
    //             .where(field("slot_id").eq(id))
    // }

    private ParkingSlotDto mapToDto(Record record) {
        return new ParkingSlotDto(
                record.get("slot_id", Integer.class),
                record.get("location_id", String.class),
                record.get("display_code", String.class),
                record.get("vehicle_type_id", Integer.class),
                record.get("reservation_class_id", Integer.class),
                record.get("sensor_id", String.class),
                record.get("current_status", String.class)
                // record.get("slot_version", Integer.class),
                // record.get("updated_at", LocalDateTime.class)
        );
    }
}
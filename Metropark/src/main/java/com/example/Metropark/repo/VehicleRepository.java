package com.example.Metropark.repo;

import com.example.Metropark.dto.VehicleDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class VehicleRepository {

    private final DSLContext dsl;

    public VehicleRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(VehicleDto dto) {
        return Mono.from(dsl.insertInto(table("vehicles"))
                .columns(
                        field("user_id"), field("vehicle_number"), field("vehicle_type_id"),
                        field("brand"), field("model"), field("color"),
                        field("is_active"), field("created_at"), field("updated_at"))
                .values(
                        dto.userId(), dto.vehicleNumber(), dto.vehicleTypeId(),
                        dto.brand(), dto.model(), dto.color(),
                        dto.isActive(), dto.createdAt(), dto.updatedAt()));
    }

    public Flux<VehicleDto> findAll() {
        return Flux.from(dsl.selectFrom(table("vehicles"))).map(this::mapToDto);
    }

    public Mono<VehicleDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("vehicles"))
                .where(field("vehicle_id").eq(id)))
                .map(this::mapToDto);
    }

    // Soft Delete / Toggle Active Status
    public Mono<Integer> updateActiveStatus(Integer id, Boolean isActive) {
        return Mono.from(dsl.update(table("vehicles"))
                .set(field("is_active"), isActive)
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("vehicle_id").eq(id)));
    }

    private VehicleDto mapToDto(Record record) {
        return new VehicleDto(
                record.get("vehicle_id", Integer.class),
                record.get("user_id", String.class),
                record.get("vehicle_number", String.class),
                record.get("vehicle_type_id", Integer.class),
                record.get("brand", String.class),
                record.get("model", String.class),
                record.get("color", String.class),
                record.get("is_active", Boolean.class),
                record.get("created_at", LocalDateTime.class),
                record.get("updated_at", LocalDateTime.class));
    }
}
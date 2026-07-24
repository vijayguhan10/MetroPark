package com.example.Metropark.vehicle.repo;

import java.time.LocalDateTime;

import org.jooq.DSLContext;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.dto.VehicleResponseDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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

    public Flux<VehicleResponseDto> findAll() {
        var query = dsl.select(
                field("vehicles.vehicle_id", Integer.class).as("vehicle_id"),
                field("vehicles.user_id", String.class).as("user_id"),
                field("users.name", String.class).as("user_name"),
                field("vehicles.vehicle_number", String.class).as("vehicle_number"),
                field("vehicles.vehicle_type_id", Integer.class).as("vehicle_type_id"),
                field("vehicle_types.type_display_name", String.class).as("vehicle_type_name"),
                field("vehicles.brand", String.class).as("brand"),
                field("vehicles.model", String.class).as("model"),
                field("vehicles.color", String.class).as("color"),
                field("vehicles.is_active", Boolean.class).as("is_active"),
                field("vehicles.created_at", LocalDateTime.class).as("created_at"),
                field("vehicles.updated_at", LocalDateTime.class).as("updated_at"))
                .from(table("vehicles"))
                .leftJoin(table("vehicle_types"))
                .on(field("vehicles.vehicle_type_id").eq(field("vehicle_types.vehicle_type_id")))
                .leftJoin(table("users"))
                .on(field("vehicles.user_id").eq(field("users.user_id")));

        return Flux.from(query).map(record -> record.into(VehicleResponseDto.class));
    }

    public Mono<VehicleDto> findById(Integer id) {
        var query = dsl.select(
                field("vehicle_id").as("vehicle_id"),
                field("user_id").as("user_id"),
                field("vehicle_number").as("vehicle_number"),
                field("vehicle_type_id").as("vehicle_type_id"),
                field("brand").as("brand"),
                field("model").as("model"),
                field("color").as("color"),
                field("is_active").as("is_active"),
                field("created_at").as("created_at"),
                field("updated_at").as("updated_at"))
                .from(table("vehicles"))
                .where(field("vehicle_id").eq(id));

        return Mono.from(query).map(record -> record.into(VehicleDto.class));
    }

    public Mono<Integer> updateActiveStatus(Integer id, Boolean isActive) {
        return Mono.from(dsl.update(table("vehicles"))
                .set(field("is_active"), isActive)
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("vehicle_id").eq(id)));
    }
}
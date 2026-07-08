package com.example.Metropark.location.repo;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.location.dto.LocationTypeDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class LocationTypeRepository {

    private final DSLContext dsl;

    public LocationTypeRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    // CREATE
    public Mono<Integer> create(LocationTypeDto dto) {
        return Mono.from(dsl.insertInto(table("location_types"))
                .columns(field("type_name"))
                .values(dto.typeName()));
    }

    // READ ALL
    public Flux<LocationTypeDto> findAll() {
        return Flux.from(dsl.selectFrom(table("location_types")))
                .map(this::mapToDto);
    }

    // READ BY ID
    public Mono<LocationTypeDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("location_types"))
                .where(field("type_id").eq(id)))
                .map(this::mapToDto);
    }

    // UPDATE
    public Mono<Integer> update(Integer id, LocationTypeDto dto) {
        return Mono.from(dsl.update(table("location_types"))
                .set(field("type_name"), dto.typeName())
                .where(field("type_id").eq(id)));
    }

    // DELETE
    public Mono<Integer> delete(Integer id) {
        return Mono.from(dsl.deleteFrom(table("location_types"))
                .where(field("type_id").eq(id)));
    }

    // MAPPER
    private LocationTypeDto mapToDto(Record record) {
        return new LocationTypeDto(
                record.get("type_id", Long.class),
                record.get("type_name", String.class)
        );
    }
}
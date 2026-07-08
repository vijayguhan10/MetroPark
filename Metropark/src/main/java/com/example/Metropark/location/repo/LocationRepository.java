package com.example.Metropark.location.repo;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.location.dto.LocationDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class LocationRepository {

    private final DSLContext dsl;

    public LocationRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(LocationDto dto) {
        return Mono.from(dsl.insertInto(table("locations"))
                .columns(
                        field("location_id"),
                        field("type_id"),
                        field("location_name"),
                        field("city"),
                        field("status"))
                .values(
                        dto.locationId(),
                        dto.typeId(),
                        dto.locationName(),
                        dto.city(),
                        dto.status()));
    }

    public Flux<LocationDto> findAll() {
        return Flux.from(dsl.selectFrom(table("locations")))
                .map(this::mapToDto);
    }

    public Mono<LocationDto> findById(String id) {
        return Mono.from(dsl.selectFrom(table("locations"))
                .where(field("location_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<Integer> updateStatus(String id, String status) {
        return Mono.from(dsl.update(table("locations"))
                .set(field("status"), status)
                .where(field("location_id").eq(id)));
    }

    private LocationDto mapToDto(Record record) {
        return new LocationDto(
                record.get("location_id", String.class),
                record.get("type_id", Integer.class),
                record.get("location_name", String.class),
                record.get("city", String.class),
                record.get("status", String.class));
    }
}
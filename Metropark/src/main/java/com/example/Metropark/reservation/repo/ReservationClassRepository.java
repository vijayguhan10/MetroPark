package com.example.Metropark.reservation.repo;

import com.example.Metropark.reservation.dto.ReservationClassDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class ReservationClassRepository {

    private final DSLContext dsl;

    public ReservationClassRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(ReservationClassDto dto) {
        return Mono.from(dsl.insertInto(table("reservation_classes"))
                .columns(field("class_name"))
                .values(dto.className()));
    }

    public Flux<ReservationClassDto> findAll() {
        return Flux.from(dsl.selectFrom(table("reservation_classes")))
                .map(this::mapToDto);
    }

    public Mono<ReservationClassDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("reservation_classes"))
                .where(field("class_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<Integer> update(Integer id, ReservationClassDto dto) {
        return Mono.from(dsl.update(table("reservation_classes"))
                .set(field("class_name"), dto.className())
                .where(field("class_id").eq(id)));
    }

    public Mono<Integer> delete(Integer id) {
        return Mono.from(dsl.deleteFrom(table("reservation_classes"))
                .where(field("class_id").eq(id)));
    }

    private ReservationClassDto mapToDto(Record record) {
        return new ReservationClassDto(
                record.get("class_id", Integer.class),
                record.get("class_name", String.class)
        );
    }
}
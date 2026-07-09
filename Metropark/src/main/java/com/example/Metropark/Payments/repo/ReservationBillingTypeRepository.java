package com.example.Metropark.payments.repo;

import java.time.LocalDateTime;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.payments.dto.ReservationBillingTypeDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class ReservationBillingTypeRepository {

    private final DSLContext dsl;

    public ReservationBillingTypeRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(ReservationBillingTypeDto dto) {
        return Mono.from(dsl.insertInto(table("reservation_billing_types"))
                .columns(
                        field("type_name"), field("duration_value"), field("duration_unit"),
                        field("is_subscription"), field("created_at"), field("updated_at"))
                .values(
                        dto.typeName(), dto.durationValue(), dto.durationUnit(),
                        dto.isSubscription(), dto.createdAt(), dto.updatedAt()));
    }

    public Flux<ReservationBillingTypeDto> findAll() {
        return Flux.from(dsl.selectFrom(table("reservation_billing_types"))).map(this::mapToDto);
    }

    public Mono<ReservationBillingTypeDto> findById(Long id) {
        return Mono.from(dsl.selectFrom(table("reservation_billing_types"))
                .where(field("billing_type_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<ReservationBillingTypeDto> findByTypeName(String typeName) {
        return Mono.from(dsl.selectFrom(table("reservation_billing_types"))
                .where(field("type_name").eq(typeName)))
                .map(this::mapToDto);
    }

    public Mono<Integer> update(Long id, ReservationBillingTypeDto dto) {
        return Mono.from(dsl.update(table("reservation_billing_types"))
                .set(field("type_name"), dto.typeName())
                .set(field("duration_value"), dto.durationValue())
                .set(field("duration_unit"), dto.durationUnit())
                .set(field("is_subscription"), dto.isSubscription())
                .set(field("updated_at"), dto.updatedAt())
                .where(field("billing_type_id").eq(id)));
    }

    public Mono<Integer> delete(Long id) {
        return Mono.from(dsl.deleteFrom(table("reservation_billing_types"))
                .where(field("billing_type_id").eq(id)));
    }

    public Mono<Boolean> existsByTypeName(String typeName) {
        return Mono.fromSupplier(() -> dsl.fetchExists(
                dsl.selectOne().from(table("reservation_billing_types"))
                        .where(field("type_name").eq(typeName))));
    }

    private ReservationBillingTypeDto mapToDto(Record record) {
        return new ReservationBillingTypeDto(
                record.get("billing_type_id", Long.class),
                record.get("type_name", String.class),
                record.get("duration_value", Integer.class),
                record.get("duration_unit", String.class),
                record.get("is_subscription", Boolean.class),
                record.get("created_at", LocalDateTime.class),
                record.get("updated_at", LocalDateTime.class));
    }
}
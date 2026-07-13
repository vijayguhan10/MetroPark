package com.example.Metropark.payments.repo;

import java.time.LocalDateTime;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.payments.dto.PaymentMethodDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class PaymentMethodRepository {

    private final DSLContext dsl;

    public PaymentMethodRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(PaymentMethodDto dto) {
        return Mono.from(dsl.insertInto(table("payment_methods"))
                .columns(field("method_name"), field("is_active"), field("created_at"))
                .values(dto.methodName(), dto.isActive(), dto.createdAt()));
    }

    public Flux<PaymentMethodDto> findAll() {
        return Flux.from(dsl.selectFrom(table("payment_methods"))).map(this::mapToDto);
    }

    public Mono<PaymentMethodDto> findById(Long id) {
        return Mono.from(dsl.selectFrom(table("payment_methods"))
                .where(field("method_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<PaymentMethodDto> findByMethodName(String methodName) {
        return Mono.from(dsl.selectFrom(table("payment_methods"))
                .where(field("method_name").eq(methodName)))
                .map(this::mapToDto);
    }

    public Mono<Integer> update(Long id, PaymentMethodDto dto) {
        return Mono.from(dsl.update(table("payment_methods"))
                .set(field("method_name"), dto.methodName())
                .set(field("is_active"), dto.isActive())
                .where(field("method_id").eq(id)));
    }

    public Mono<Integer> delete(Long id) {
        return Mono.from(dsl.deleteFrom(table("payment_methods"))
                .where(field("method_id").eq(id)));
    }

    public Mono<Boolean> existsByMethodName(String methodName) {
        return Mono.from(dsl.selectOne()
                .from(table("payment_methods"))
                .where(field("method_name").eq(methodName)))
                .map(record -> true)
                .defaultIfEmpty(false);
    }

    private PaymentMethodDto mapToDto(Record record) {
        return new PaymentMethodDto(
                record.get("method_id", Long.class),
                record.get("method_name", String.class),
                record.get("is_active", Boolean.class),
                record.get("created_at", LocalDateTime.class));
    }
}
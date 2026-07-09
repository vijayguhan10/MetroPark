package com.example.Metropark.payments.repo;

import java.time.LocalDateTime;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.payments.dto.PaymentHistoryDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class PaymentHistoryRepository {

    private final DSLContext dsl;

    public PaymentHistoryRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(PaymentHistoryDto dto) {
        return Mono.from(dsl.insertInto(table("payment_history"))
                .columns(
                        field("payment_id"), field("previous_status"), field("current_status"),
                        field("changed_by"), field("reason"), field("gateway_reference"), field("changed_at"))
                .values(
                        dto.paymentId(), dto.previousStatus(), dto.currentStatus(),
                        dto.changedBy(), dto.reason(), dto.gatewayReference(), dto.changedAt()));
    }

    public Flux<PaymentHistoryDto> findAll() {
        return Flux.from(dsl.selectFrom(table("payment_history"))).map(this::mapToDto);
    }

    public Mono<PaymentHistoryDto> findById(Long id) {
        return Mono.from(dsl.selectFrom(table("payment_history"))
                .where(field("history_id").eq(id)))
                .map(this::mapToDto);
    }

    public Flux<PaymentHistoryDto> findByPaymentId(Long paymentId) {
        return Flux.from(dsl.selectFrom(table("payment_history"))
                .where(field("payment_id").eq(paymentId))
                .orderBy(field("changed_at").desc()))
                .map(this::mapToDto);
    }

    private PaymentHistoryDto mapToDto(Record record) {
        return new PaymentHistoryDto(
                record.get("history_id", Long.class),
                record.get("payment_id", Long.class),
                record.get("previous_status", String.class),
                record.get("current_status", String.class),
                record.get("changed_by", String.class),
                record.get("reason", String.class),
                record.get("gateway_reference", String.class),
                record.get("changed_at", LocalDateTime.class));
    }
}
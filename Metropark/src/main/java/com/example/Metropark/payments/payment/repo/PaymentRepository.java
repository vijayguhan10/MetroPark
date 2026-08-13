package com.example.Metropark.payments.payment.repo;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.payments.payment.dto.PaymentDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class PaymentRepository {

        private final DSLContext dsl;

        public PaymentRepository(DSLContext dsl) {
                this.dsl = dsl;
        }

        public Mono<Long> allocatePaymentId() {
                return Mono.from(dsl.select(
                                field("nextval(pg_get_serial_sequence('payments', 'payment_id'))", Long.class)))
                                .map(record -> record.get(0, Long.class));
        }

        public Mono<Integer> create(PaymentDto dto) {
                return create(dto, null);
        }

        public Mono<Integer> create(PaymentDto dto, Long explicitId) {
                if (explicitId != null) {
                        return Mono.from(dsl.insertInto(table("payments"))
                                        .columns(
                                                        field("payment_id"), field("transaction_reference"),
                                                        field("session_id"), field("user_id"),
                                                        field("method_id"), field("amount"), field("currency"),
                                                        field("payment_status"),
                                                        field("gateway_response_code"),
                                                        field("gateway_response_message"), field("processed_at"),
                                                        field("created_at"), field("updated_at"))
                                        .values(
                                                        explicitId, dto.transactionReference(), dto.sessionId(),
                                                        dto.userId(),
                                                        dto.methodId(), dto.amount(), dto.currency(),
                                                        dto.paymentStatus(),
                                                        dto.gatewayResponseCode(), dto.gatewayResponseMessage(),
                                                        dto.processedAt(),
                                                        dto.createdAt(), dto.updatedAt())
                                        .returning(field("payment_id")))
                                        .map(record -> record.get(field("payment_id"), Integer.class));
                }

                return Mono.from(dsl.insertInto(table("payments"))
                                .columns(
                                                field("transaction_reference"), field("session_id"), field("user_id"),
                                                field("method_id"), field("amount"), field("currency"),
                                                field("payment_status"),
                                                field("gateway_response_code"), field("gateway_response_message"),
                                                field("processed_at"),
                                                field("created_at"), field("updated_at"))
                                .values(
                                                dto.transactionReference(), dto.sessionId(), dto.userId(),
                                                dto.methodId(), dto.amount(), dto.currency(), dto.paymentStatus(),
                                                dto.gatewayResponseCode(), dto.gatewayResponseMessage(),
                                                dto.processedAt(),
                                                dto.createdAt(), dto.updatedAt())
                                .returning(field("payment_id")))
                                .map(record -> record.get(field("payment_id"), Integer.class));
        }

        public Flux<PaymentDto> findAll() {
                return Flux.from(dsl.selectFrom(table("payments"))).map(this::mapToDto);
        }

        public Mono<PaymentDto> findById(Long id) {
                return Mono.from(dsl.selectFrom(table("payments"))
                                .where(field("payment_id").eq(id)))
                                .map(this::mapToDto);
        }

        public Mono<PaymentDto> findByTransactionReference(String transactionReference) {
                return Mono.from(dsl.selectFrom(table("payments"))
                                .where(field("transaction_reference").eq(transactionReference)))
                                .map(this::mapToDto);
        }

        public Flux<PaymentDto> findBySessionId(Integer sessionId) {
                return Flux.from(dsl.selectFrom(table("payments"))
                                .where(field("session_id").eq(sessionId)))
                                .map(this::mapToDto);
        }

        public Mono<Integer> updateStatus(
                        Long id,
                        String expectedStatus,
                        String newStatus,
                        LocalDateTime processedAt,
                        LocalDateTime updatedAt) {

                return Mono.from(dsl.update(table("payments"))
                                .set(field("payment_status"), newStatus)
                                .set(field("processed_at"), processedAt)
                                .set(field("updated_at"), updatedAt)
                                .where(field("payment_id").eq(id))
                                .and(field("payment_status").eq(expectedStatus)))
                                .defaultIfEmpty(0);
        }

        public Mono<Integer> updateStatusById(
                        Long id,
                        String newStatus,
                        LocalDateTime processedAt,
                        LocalDateTime updatedAt) {

                return Mono.from(dsl.update(table("payments"))
                                .set(field("payment_status"), newStatus)
                                .set(field("processed_at"), processedAt)
                                .set(field("updated_at"), updatedAt)
                                .where(field("payment_id").eq(id)))
                                .defaultIfEmpty(0);
        }

        public Mono<Boolean> existsById(Long id) {
                return Mono.from(dsl.selectOne()
                                .from(table("payments"))
                                .where(field("payment_id").eq(id)))
                                .map(record -> true)
                                .defaultIfEmpty(false);
        }

        public Mono<Integer> updateGatewayResponse(
                        Long id,
                        String expectedStatus,
                        String newStatus,
                        String transactionReference,
                        String gatewayResponseCode,
                        String gatewayResponseMessage,
                        LocalDateTime processedAt,
                        LocalDateTime updatedAt) {

                return Mono.from(dsl.update(table("payments"))
                                .set(field("transaction_reference"), transactionReference)
                                .set(field("gateway_response_code"), gatewayResponseCode)
                                .set(field("gateway_response_message"), gatewayResponseMessage)
                                .set(field("payment_status"), newStatus)
                                .set(field("processed_at"), processedAt)
                                .set(field("updated_at"), updatedAt)
                                .where(field("payment_id").eq(id))
                                .and(field("payment_status").eq(expectedStatus)))
                                .defaultIfEmpty(0);
        }

        public Mono<Boolean> existsByTransactionReference(String transactionReference) {
                return Mono.from(dsl.selectOne()
                                .from(table("payments"))
                                .where(field("transaction_reference").eq(transactionReference)))
                                .map(record -> true)
                                .defaultIfEmpty(false);
        }

        private PaymentDto mapToDto(Record record) {
                return new PaymentDto(
                                record.get("payment_id", Long.class),
                                record.get("transaction_reference", String.class),
                                record.get("session_id", Integer.class),
                                record.get("user_id", String.class),
                                record.get("method_id", Integer.class),
                                record.get("amount", BigDecimal.class),
                                record.get("currency", String.class),
                                record.get("payment_status", String.class),
                                record.get("gateway_response_code", String.class),
                                record.get("gateway_response_message", String.class),
                                record.get("processed_at", LocalDateTime.class),
                                record.get("created_at", LocalDateTime.class),
                                record.get("updated_at", LocalDateTime.class));
        }
}

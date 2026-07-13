package com.example.Metropark.payments.repo;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.payments.dto.PricingRateDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public class PricingRateRepository {

        private final DSLContext dsl;

        public PricingRateRepository(DSLContext dsl) {
                this.dsl = dsl;
        }

        public Mono<Integer> create(PricingRateDto dto) {
                return Mono.from(dsl.insertInto(table("pricing_rates"))
                                .columns(
                                                field("location_id"), field("vehicle_type_id"),
                                                field("base_rate"), field("currency"),
                                                field("effective_from"), field("effective_to"), field("is_active"),
                                                field("created_at"), field("updated_at"))
                                .values(
                                                dto.locationId(), dto.vehicleTypeId(),
                                                dto.baseRate(), dto.currency(),
                                                dto.effectiveFrom(), dto.effectiveTo(), dto.isActive(),
                                                dto.createdAt(), dto.updatedAt()));
        }

        public Flux<PricingRateDto> findAll() {
                return Flux.from(dsl.selectFrom(table("pricing_rates"))).map(this::mapToDto);
        }

        public Mono<PricingRateDto> findById(Long id) {
                return Mono.from(dsl.selectFrom(table("pricing_rates"))
                                .where(field("rate_id").eq(id)))
                                .map(this::mapToDto);
        }

        public Flux<PricingRateDto> findByCombination(
                        String locationId,
                        Integer vehicleTypeId) {

                return Flux.from(dsl.selectFrom(table("pricing_rates"))
                                .where(field("location_id").eq(locationId))
                                .and(field("vehicle_type_id").eq(vehicleTypeId))
                                .orderBy(field("effective_from").desc()))
                                .map(this::mapToDto);
        }

        public Mono<PricingRateDto> findEffectiveRate(
                        String locationId,
                        Integer vehicleTypeId,
                        LocalDateTime atTime) {

                Condition activeWindow = field("effective_from").le(atTime)
                                .and(field("effective_to").isNull().or(field("effective_to").ge(atTime)));

                return Mono.from(dsl.selectFrom(table("pricing_rates"))
                                .where(field("location_id").eq(locationId))
                                .and(field("vehicle_type_id").eq(vehicleTypeId))
                                .and(field("is_active").eq(Boolean.TRUE))
                                .and(activeWindow)
                                .orderBy(field("effective_from").desc())
                                .limit(1))
                                .map(this::mapToDto);
        }

        public Mono<Integer> update(Long id, PricingRateDto dto) {
                return Mono.from(dsl.update(table("pricing_rates"))
                                .set(field("location_id"), dto.locationId())
                                .set(field("vehicle_type_id"), dto.vehicleTypeId())
                                .set(field("base_rate"), dto.baseRate())
                                .set(field("currency"), dto.currency())
                                .set(field("effective_from"), dto.effectiveFrom())
                                .set(field("effective_to"), dto.effectiveTo())
                                .set(field("is_active"), dto.isActive())
                                .set(field("updated_at"), dto.updatedAt())
                                .where(field("rate_id").eq(id)));
        }

        public Mono<Integer> delete(Long id) {
                return Mono.from(dsl.deleteFrom(table("pricing_rates"))
                                .where(field("rate_id").eq(id)));
        }

        public Mono<Boolean> hasConflictingRate(
                        String locationId,
                        Integer vehicleTypeId,
                        LocalDateTime effectiveFrom,
                        LocalDateTime effectiveTo,
                        Long excludeRateId) {

                Condition baseConditions = field("location_id").eq(locationId)
                                .and(field("vehicle_type_id").eq(vehicleTypeId))
                                .and(field("is_active").eq(Boolean.TRUE));

                Condition overlapConditions = field("effective_from")
                                .le(effectiveTo == null ? LocalDateTime.MAX : effectiveTo)
                                .and(field("effective_to").isNull().or(field("effective_to").ge(effectiveFrom)));

                final Condition query = excludeRateId == null
                                ? baseConditions.and(overlapConditions)
                                : baseConditions.and(overlapConditions).and(field("rate_id").ne(excludeRateId));

                return Mono.from(dsl.selectOne()
                                .from(table("pricing_rates"))
                                .where(query))
                                .map(record -> true)
                                .defaultIfEmpty(false);
        }

        private PricingRateDto mapToDto(Record record) {
                return new PricingRateDto(
                                record.get("rate_id", Long.class),
                                record.get("location_id", String.class),
                                record.get("vehicle_type_id", Integer.class),
                                record.get("base_rate", BigDecimal.class),
                                record.get("currency", String.class),
                                record.get("effective_from", LocalDateTime.class),
                                record.get("effective_to", LocalDateTime.class),
                                record.get("is_active", Boolean.class),
                                record.get("created_at", LocalDateTime.class),
                                record.get("updated_at", LocalDateTime.class));
        }
}
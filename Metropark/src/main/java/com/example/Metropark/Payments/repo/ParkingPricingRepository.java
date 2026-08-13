package com.example.Metropark.payments.repo;

import java.math.BigDecimal;

import org.jooq.DSLContext;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Mono;

@Repository
public class ParkingPricingRepository {

    private final DSLContext dsl;

    public ParkingPricingRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<BigDecimal> getBaseRatePerHour() {
        return Mono.from(
            dsl.select(field("base_rate_per_hour"))
                .from(table("parking_pricing"))
                .orderBy(field("pricing_id").desc())
                .limit(1)
        ).map(record -> record.get(field("base_rate_per_hour"), BigDecimal.class))
        .defaultIfEmpty(new BigDecimal("4.00"));
    }
}

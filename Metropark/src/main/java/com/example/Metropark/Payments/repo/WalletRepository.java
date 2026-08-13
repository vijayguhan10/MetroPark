package com.example.Metropark.payments.repo;

import java.math.BigDecimal;

import org.jooq.DSLContext;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import reactor.core.publisher.Mono;

@Repository
public class WalletRepository {

    private final DSLContext dsl;

    public WalletRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Atomically deduct amount from user's wallet fund.
     * SQL: UPDATE wallet_fund SET fund = fund - :amount WHERE user_id = :userId AND fund >= :amount;
     * Returns 1 if deduction succeeded, 0 if failed (insufficient funds or user not found).
     */
    public Mono<Integer> deductFund(String userId, BigDecimal amount) {
        return Mono.from(
            dsl.update(table("wallet_fund"))
                .set(field("fund", BigDecimal.class), field("fund", BigDecimal.class).sub(amount))
                .where(field("user_id").eq(userId))
                .and(field("fund", BigDecimal.class).ge(amount))
        ).defaultIfEmpty(0);
    }

    public Mono<BigDecimal> getFund(String userId) {
        return Mono.from(
            dsl.select(field("fund"))
                .from(table("wallet_fund"))
                .where(field("user_id").eq(userId))
        ).map(record -> record.get(field("fund"), BigDecimal.class))
        .defaultIfEmpty(BigDecimal.ZERO);
    }

    public Mono<Integer> addFund(String userId, BigDecimal amount) {
        return Mono.from(
            dsl.insertInto(table("wallet_fund"))
                .columns(field("user_id"), field("fund"))
                .values(userId, amount)
                .onConflict(field("user_id"))
                .doUpdate()
                .set(field("fund", BigDecimal.class), field("wallet_fund.fund", BigDecimal.class).add(amount))
        ).defaultIfEmpty(0);
    }
}

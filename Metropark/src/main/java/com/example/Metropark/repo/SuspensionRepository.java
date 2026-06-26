package com.example.Metropark.repo;
import com.example.Metropark.dto.SuspensionDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class SuspensionRepository {

    private final DSLContext dsl;

    public SuspensionRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Flux<SuspensionDto> findActiveSuspensionsByUserId(String userId) {
        return Flux.from(dsl.selectFrom(table("user_suspensions"))
                .where(field("user_id").eq(userId))
                .and(field("is_active").eq(true)))
                .map(this::mapToDto);
    }

    public Mono<Integer> createSuspension(SuspensionDto dto) {
        return Mono.from(dsl.insertInto(table("user_suspensions"))
                .columns(
                        field("user_id"), field("reason"), field("suspended_at"),
                        field("suspended_until"), field("is_active"), field("authorized_by")
                )
                .values(
                        dto.userId(), dto.reason(), dto.suspendedAt(),
                        dto.suspendedUntil(), dto.isActive(), dto.authorizedBy()
                ));
    }

    private SuspensionDto mapToDto(Record record) {
        return new SuspensionDto(
                record.get("suspension_id", Integer.class),
                record.get("user_id", String.class),
                record.get("reason", String.class),
                record.get("suspended_at", LocalDateTime.class),
                record.get("suspended_until", LocalDateTime.class),
                record.get("is_active", Boolean.class),
                record.get("authorized_by", String.class)
        );
    }
}
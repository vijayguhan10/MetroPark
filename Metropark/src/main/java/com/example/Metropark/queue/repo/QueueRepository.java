package com.example.Metropark.queue.repo;

import com.example.Metropark.queue.dto.QueueDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class QueueRepository {

    private final DSLContext dsl;

    public QueueRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(QueueDto dto) {
        return Mono.from(dsl.insertInto(table("queues"))
                .columns(
                        field("location_id"), field("vehicle_type_id"), 
                        field("reservation_class_id"), field("queue_name"), 
                        field("status"), field("created_at")
                )
                .values(
                        dto.locationId(), dto.vehicleTypeId(), 
                        dto.reservationClassId(), dto.queueName(), 
                        dto.status(), dto.createdAt()
                ));
    }

    public Flux<QueueDto> findAll() {
        return Flux.from(dsl.selectFrom(table("queues"))).map(this::mapToDto);
    }

    public Mono<QueueDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("queues"))
                .where(field("queue_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<Integer> updateStatus(Integer id, String status) {
        return Mono.from(dsl.update(table("queues"))
                .set(field("status"), status)
                .where(field("queue_id").eq(id)));
    }

    private QueueDto mapToDto(Record record) {
        return new QueueDto(
                record.get("queue_id", Integer.class),
                record.get("location_id", String.class),
                record.get("vehicle_type_id", Integer.class),
                record.get("reservation_class_id", Integer.class),
                record.get("queue_name", String.class),
                record.get("status", String.class),
                record.get("created_at", LocalDateTime.class)
        );
    }
}
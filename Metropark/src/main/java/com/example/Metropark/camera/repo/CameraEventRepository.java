package com.example.Metropark.camera.repo;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

import java.time.LocalDateTime;
import java.util.UUID;

import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.event.CameraEventMapper;
import com.example.Metropark.camera.event.CameraEventStatus;

import reactor.core.publisher.Mono;

/**
 * The {@code camera_events} audit log.
 *
 * <p>
 * Writes only. Nothing in the parking path reads a camera event back - the event
 * itself travels over RabbitMQ - so this exists to make the observation durable
 * and its outcome inspectable, not to feed business logic.
 */
@Repository
public class CameraEventRepository {

    private final DSLContext dsl;

    public CameraEventRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Records the observation as RECEIVED.
     *
     * <p>
     * {@code ON CONFLICT (event_id) DO NOTHING} keeps a redelivered or replayed
     * event from failing on the primary key. Targeted at the key rather than
     * blanket, so any other constraint violation still surfaces instead of being
     * swallowed into a zero row count.
     */
    public Mono<Integer> save(CameraEvent event) {
        return Mono.from(dsl.insertInto(table("camera_events"))
                .columns(
                        field("event_id"),
                        field("event_type"),
                        field("license_plate"),
                        field("vehicle_id"),
                        field("user_id"),
                        field("parking_lot_id"),
                        field("camera_id"),
                        field("status"),
                        field("event_timestamp"),
                        field("created_at"),
                        field("updated_at"))
                .values(
                        event.eventId(),
                        event.eventType().name(),
                        event.licensePlate(),
                        event.vehicleId(),
                        event.userId(),
                        event.parkingLotId(),
                        event.cameraId(),
                        CameraEventStatus.RECEIVED.name(),
                        CameraEventMapper.toColumn(event.timestamp()),
                        LocalDateTime.now(),
                        LocalDateTime.now())
                .onConflict(field("event_id")).doNothing())
                .defaultIfEmpty(0);
    }

    public Mono<Integer> updateStatus(UUID eventId, CameraEventStatus status) {
        return updateStatus(eventId, status, null);
    }

    /**
     * Moves RECEIVED to PROCESSING, and does nothing to anything else.
     *
     * <p>
     * The guard is what makes the two consumers safe to run independently. They
     * read the same exchange through separate queues in no particular order, so the
     * audit consumer routinely gets its copy AFTER the processing consumer has
     * already finished and written PROCESSED or FAILED. An unguarded update would
     * drag that terminal status back to PROCESSING and leave the audit log claiming
     * work is still in flight forever.
     */
    public Mono<Integer> markProcessingIfReceived(UUID eventId) {
        return Mono.from(dsl.update(table("camera_events"))
                .set(field("status"), CameraEventStatus.PROCESSING.name())
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("event_id").eq(eventId))
                .and(field("status").eq(CameraEventStatus.RECEIVED.name())))
                .defaultIfEmpty(0);
    }

    /**
     * Moves one event to its next status.
     *
     * <p>
     * {@code failureReason} is only meaningful for FAILED; it is written
     * unconditionally so that an event which fails, is retried and then succeeds
     * does not keep the stale reason from its first attempt.
     */
    public Mono<Integer> updateStatus(UUID eventId, CameraEventStatus status, String failureReason) {
        return Mono.from(dsl.update(table("camera_events"))
                .set(field("status"), status.name())
                .set(field("failure_reason"), truncate(failureReason))
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("event_id").eq(eventId)))
                .defaultIfEmpty(0);
    }

    /** failure_reason is varchar(500); an overlong stack message must not abort the audit write. */
    private static String truncate(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 500 ? reason : reason.substring(0, 500);
    }
}

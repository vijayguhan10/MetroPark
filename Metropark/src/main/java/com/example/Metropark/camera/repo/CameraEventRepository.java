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

@Repository
public class CameraEventRepository {

    private final DSLContext dsl;

    public CameraEventRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

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

    public Mono<Integer> markProcessingIfReceived(UUID eventId) {
        return Mono.from(dsl.update(table("camera_events"))
                .set(field("status"), CameraEventStatus.PROCESSING.name())
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("event_id").eq(eventId))
                .and(field("status").eq(CameraEventStatus.RECEIVED.name())))
                .defaultIfEmpty(0);
    }

    public Mono<Integer> updateStatus(UUID eventId, CameraEventStatus status, String failureReason) {
        return Mono.from(dsl.update(table("camera_events"))
                .set(field("status"), status.name())
                .set(field("failure_reason"), truncate(failureReason))
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("event_id").eq(eventId)))
                .defaultIfEmpty(0);
    }

    private static String truncate(String reason) {
        if (reason == null) {
            return null;
        }
        return reason.length() <= 500 ? reason : reason.substring(0, 500);
    }
}

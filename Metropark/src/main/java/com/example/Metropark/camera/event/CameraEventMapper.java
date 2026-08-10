package com.example.Metropark.camera.event;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import org.jooq.Record;

/**
 * Translates between the wire form of a camera event and its
 * {@code camera_events} row.
 *
 * <p>
 * The one thing worth stating: {@link CameraEvent#timestamp()} is an
 * {@link Instant} while {@code event_timestamp} is {@code timestamp without time
 * zone}, so every crossing goes through the system zone - the same zone every
 * {@code LocalDateTime.now()} elsewhere in this codebase already uses. Binding
 * an Instant straight at the column would silently store UTC and put camera
 * events an offset apart from the sessions they create.
 */
public final class CameraEventMapper {

    private CameraEventMapper() {
    }

    public static LocalDateTime toColumn(Instant timestamp) {
        return timestamp == null
                ? LocalDateTime.now()
                : LocalDateTime.ofInstant(timestamp, ZoneId.systemDefault());
    }

    public static Instant toInstant(LocalDateTime column) {
        return column == null
                ? null
                : column.atZone(ZoneId.systemDefault()).toInstant();
    }

    public static CameraEvent fromRecord(Record record) {
        return new CameraEvent(
                record.get("event_id", UUID.class),
                CameraEventType.valueOf(record.get("event_type", String.class)),
                record.get("license_plate", String.class),
                record.get("vehicle_id", Integer.class),
                record.get("user_id", String.class),
                record.get("parking_lot_id", String.class),
                record.get("camera_id", String.class),
                toInstant(record.get("event_timestamp", LocalDateTime.class)));
    }

    public static CameraEventStatus statusOf(Record record) {
        return CameraEventStatus.valueOf(record.get("status", String.class));
    }
}

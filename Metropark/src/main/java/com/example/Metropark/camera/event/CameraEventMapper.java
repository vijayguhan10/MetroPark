package com.example.Metropark.camera.event;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.UUID;

import org.jooq.Record;

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

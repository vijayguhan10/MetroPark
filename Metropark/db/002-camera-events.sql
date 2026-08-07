-- The durable record of every ANPR camera observation.
--
-- This is the audit log that CameraEventAuditConsumer maintains and the reason
-- a camera event can be replayed: SimulationService persists the event BEFORE
-- publishing it, so a crash between the write and the broker leaves a RECEIVED
-- row rather than a silently lost car.
--
-- Deliberately carries NO foreign keys to vehicles, users or locations. A camera
-- reads a number plate off a car; it does not know whether that plate is
-- registered. An unrecognised plate must still be recorded (and later marked
-- FAILED by the processing consumer), which an FK would prevent by rejecting the
-- insert outright.

CREATE TABLE IF NOT EXISTS camera_events (
    event_id         UUID PRIMARY KEY,
    event_type       VARCHAR(20)  NOT NULL,
    license_plate    VARCHAR(20)  NOT NULL,
    vehicle_id       INTEGER,
    user_id          VARCHAR(20),
    parking_lot_id   VARCHAR(50),
    camera_id        VARCHAR(100),
    status           VARCHAR(20)  NOT NULL DEFAULT 'RECEIVED',
    failure_reason   VARCHAR(500),
    event_timestamp  TIMESTAMP    NOT NULL,
    created_at       TIMESTAMP    NOT NULL DEFAULT now(),
    updated_at       TIMESTAMP    NOT NULL DEFAULT now(),

    CONSTRAINT chk_camera_events_type
        CHECK (event_type IN ('CAR_ENTERED', 'CAR_EXITED')),
    CONSTRAINT chk_camera_events_status
        CHECK (status IN ('RECEIVED', 'PROCESSING', 'PROCESSED', 'FAILED'))
);

-- The audit consumer updates by primary key; the processing consumer and any
-- operator triage look events up by plate, most recent first.
CREATE INDEX IF NOT EXISTS idx_camera_events_plate_time
    ON camera_events (license_plate, event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_camera_events_status
    ON camera_events (status)
    WHERE status IN ('RECEIVED', 'PROCESSING', 'FAILED');

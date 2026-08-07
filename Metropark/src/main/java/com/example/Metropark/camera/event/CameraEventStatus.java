package com.example.Metropark.camera.event;

/**
 * Lifecycle of one row in {@code camera_events}, owned exclusively by
 * {@link com.example.Metropark.camera.consumer.CameraEventAuditConsumer}.
 *
 * <p>
 * RECEIVED is written by the producer before the event is published, so an event
 * that never reaches the broker is still on record. Everything after that is the
 * audit consumer reacting to what the processing consumer did.
 */
public enum CameraEventStatus {

    /** Persisted by the producer, not yet picked up. */
    RECEIVED,
    /** A processing consumer has taken it. */
    PROCESSING,
    /** Parking was applied, or the event was a legitimate duplicate. */
    PROCESSED,
    /** Parking could not be applied; {@code failure_reason} says why. */
    FAILED
}

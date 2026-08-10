package com.example.Metropark.event.payload;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One vehicle entry or one vehicle exit, carried as a single message.
 *
 * <p>
 * Session, slot and payment travel together because
 * {@link com.example.Metropark.parking.repo.ParkingLifecycleRepository} applies
 * them in one PostgreSQL transaction. Publishing them as three independent
 * events instead would let a slot be released in PostgreSQL while the session
 * that occupied it stayed open, which is the divergence the Redis-first design
 * is meant to rule out.
 *
 * <p>
 * {@code slot} and {@code payment} are nullable so a caller that genuinely only
 * changes the session (the plain REST status endpoints) can reuse the envelope.
 * The version pair is only meaningful on exit: the consumer updates
 * {@code session_version} from {@code expectedSessionVersion} to
 * {@code newSessionVersion} under an optimistic lock.
 */
public record ParkingLifecycleEventPayload(
        @JsonProperty("session") SessionEventPayload session,
        @JsonProperty("slot") SlotEventPayload slot,
        @JsonProperty("payment") PaymentEventPayload payment,
        @JsonProperty("expectedSessionVersion") Integer expectedSessionVersion,
        @JsonProperty("newSessionVersion") Integer newSessionVersion) {
}

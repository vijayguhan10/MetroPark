package com.example.Metropark.event.payload;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * One vehicle entry or one vehicle exit, carried as a single message.
 *
 * <p>
 * For ENTRY: Session, slot and payment travel together because
 * {@link com.example.Metropark.parking.repo.ParkingLifecycleRepository} applies
 * them in one PostgreSQL transaction.
 *
 * <p>
 * For EXIT: Only session and slot travel together in the lifecycle transaction.
 * Payment is published as a SEPARATE event (third-party) and handled by the
 * payment consumer independently.
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

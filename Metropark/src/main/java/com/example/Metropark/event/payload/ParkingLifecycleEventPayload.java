package com.example.Metropark.event.payload;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ParkingLifecycleEventPayload(
                @JsonProperty("session") SessionEventPayload session,
                @JsonProperty("slot") SlotEventPayload slot,
                @JsonProperty("payment") PaymentEventPayload payment,
                @JsonProperty("expectedSessionVersion") Integer expectedSessionVersion,
                @JsonProperty("newSessionVersion") Integer newSessionVersion) {
}

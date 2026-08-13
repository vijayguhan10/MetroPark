package com.example.Metropark.event.payload;

import com.fasterxml.jackson.annotation.JsonProperty;

public record BillingRequestedEventPayload(
    @JsonProperty("eventId") String eventId,
    @JsonProperty("sessionId") Integer sessionId,
    @JsonProperty("userId") String userId,
    @JsonProperty("vehicleId") Integer vehicleId
) {}

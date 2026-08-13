package com.example.Metropark.event;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

/**
 * Shared sink for parking lifecycle events (vehicle.entry, vehicle.exit).
 * Allows ParkingLifecycleService to emit events that can be consumed via SSE.
 */
public class ParkingEventSink {

    // Use replay().latest() to cache the latest event and replay it to late
    // subscribers
    private final Sinks.Many<Event> sink = Sinks.many().replay().latest();

    public void emit(Event event) {
        sink.tryEmitNext(event);
    }

    public Flux<Event> asFlux() {
        return sink.asFlux();
    }
}

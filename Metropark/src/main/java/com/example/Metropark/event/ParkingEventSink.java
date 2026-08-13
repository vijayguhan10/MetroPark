package com.example.Metropark.event;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;

public class ParkingEventSink {

    private final Sinks.Many<Event> sink = Sinks.many().replay().latest();

    public void emit(Event event) {
        sink.tryEmitNext(event);
    }

    public Flux<Event> asFlux() {
        return sink.asFlux();
    }
}

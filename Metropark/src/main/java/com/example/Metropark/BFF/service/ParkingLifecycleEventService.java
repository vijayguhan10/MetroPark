package com.example.Metropark.BFF.service;

import com.example.Metropark.BFF.dto.ParkingLifecycleEventDto;
import com.example.Metropark.event.Event;
import com.example.Metropark.event.ParkingEventSink;

import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;

@Service
public class ParkingLifecycleEventService {

    private final ParkingEventSink parkingEventSink;

    public ParkingLifecycleEventService(ParkingEventSink parkingEventSink) {
        this.parkingEventSink = parkingEventSink;
    }

    public Flux<ParkingLifecycleEventDto> streamParkingLifecycleEvents() {
        return parkingEventSink.asFlux()
                .map(this::convertToDto);
    }

    private ParkingLifecycleEventDto convertToDto(Event event) {
        // The payload is a ParkingLifecycleEventPayload record, Jackson will serialize it properly
        return new ParkingLifecycleEventDto(
                event.eventId(),
                event.type(),
                event.entityId(),
                event.version(),
                event.timestamp(),
                event.payload()
        );
    }
}
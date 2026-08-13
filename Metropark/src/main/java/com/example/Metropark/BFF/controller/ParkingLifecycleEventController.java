package com.example.Metropark.BFF.controller;

import com.example.Metropark.BFF.dto.ParkingLifecycleEventDto;
import com.example.Metropark.BFF.service.ParkingLifecycleEventService;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/live/parking")
public class ParkingLifecycleEventController {

    private final ParkingLifecycleEventService parkingLifecycleEventService;

    public ParkingLifecycleEventController(ParkingLifecycleEventService parkingLifecycleEventService) {
        this.parkingLifecycleEventService = parkingLifecycleEventService;
    }

    @GetMapping(value = "/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ParkingLifecycleEventDto> streamParkingLifecycleEvents() {
        return parkingLifecycleEventService.streamParkingLifecycleEvents();
    }
}

package com.example.Metropark.BFF.controller;

import com.example.Metropark.BFF.dto.SimulationEventDto;
import com.example.Metropark.BFF.dto.SimulationRunResponseDto;
import com.example.Metropark.BFF.service.SimulationService;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/bff")
public class SimulationController {

    private final SimulationService simulationService;

    public SimulationController(SimulationService simulationService) {
        this.simulationService = simulationService;
    }

    @PostMapping("/run-simulation")
    public Mono<SimulationRunResponseDto> runSimulation() {
        return simulationService.runSimulation();
    }

    @GetMapping(value = "/simulation/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<SimulationEventDto> streamSimulationEvents() {
        return simulationService.getEventStream();
    }
}

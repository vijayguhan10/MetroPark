package com.example.Metropark.BFF.controller;

import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.BFF.dto.ActiveSessionDto;
import com.example.Metropark.BFF.dto.RevenueDto;
import com.example.Metropark.BFF.service.LiveDataService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/live")
public class LiveDataController {

    private final LiveDataService liveDataService;

    public LiveDataController(LiveDataService liveDataService) {
        this.liveDataService = liveDataService;
    }

    // REST endpoints (for initial load / fallback)
    @GetMapping("/sessions")
    public Flux<ActiveSessionDto> getLiveSessions() {
        return liveDataService.getLiveSessions();
    }

    @GetMapping("/payments")
    public Mono<RevenueDto> getLivePayments() {
        return liveDataService.getLivePayments();
    }

    // Server-Sent Events (SSE) endpoints for real-time streaming
    @GetMapping(value = "/sessions/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<ActiveSessionDto> streamLiveSessions() {
        return liveDataService.streamLiveSessions();
    }

    @GetMapping(value = "/payments/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<RevenueDto> streamLivePayments() {
        return liveDataService.streamLivePayments();
    }
}
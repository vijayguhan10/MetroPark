package com.example.Metropark.gate.controller;

import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.gate.service.GateService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/gates")
public class GateController {

    private final GateService service;

    public GateController(GateService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody GateDto dto) {
        return service.createGate(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Gate created successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<GateDto> getAll() {
        return service.getAllGates();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<GateDto>> getById(@PathVariable Integer id) {
        return service.getGateById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(
            @PathVariable Integer id, 
            @RequestParam String status) {
            
        return service.updateGateStatus(id, status)
                .map(rows -> ResponseEntity.ok("Gate status updated successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class, e -> 
                        Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage())));
    }
}
package com.example.Metropark.queue.controller;

import com.example.Metropark.queue.dto.QueueDto;
import com.example.Metropark.queue.service.QueueService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/queues")
public class QueueController {

    private final QueueService service;

    public QueueController(QueueService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody QueueDto dto) {
        return service.createQueue(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Queue successfully created."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<QueueDto> getAll() {
        return service.getAllQueues();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<QueueDto>> getById(@PathVariable Integer id) {
        return service.getQueueById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PatchMapping("/{id}/status")
    public Mono<ResponseEntity<String>> updateStatus(
            @PathVariable Integer id,
            @RequestParam String status) {

        return service.updateQueueStatus(id, status)
                .map(rows -> ResponseEntity.ok("Queue status updated."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.NOT_FOUND).body(e.getMessage())));
    }
}
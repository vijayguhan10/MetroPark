package com.example.Metropark.controller;

import com.example.Metropark.dto.ParkingSlotDto;
import com.example.Metropark.service.ParkingSlotService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/parking-slots")
public class ParkingSlotController {

    private final ParkingSlotService service;

    public ParkingSlotController(ParkingSlotService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody ParkingSlotDto dto) {
        return service.createSlot(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Parking slot registered successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<ParkingSlotDto> getAll() {
        return service.getAllSlots();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ParkingSlotDto>> getById(@PathVariable Integer id) {
        return service.getSlotById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    // Patches the status using the Optimistic Lock logic
    // @PatchMapping("/{id}/status")
    // public Mono<ResponseEntity<String>> updateStatus(
    //         @PathVariable Integer id, 
    //         @RequestParam String status, 
    //         @RequestParam Integer currentVersion) {
            
    //     return service.updateSlotStatus(id, status, currentVersion)
    //             .map(rows -> ResponseEntity.ok("Slot occupancy status updated successfully."))
    //             .onErrorResume(IllegalArgumentException.class, e -> 
    //                     Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
    //             .onErrorResume(IllegalStateException.class, e -> 
    //                     Mono.just(ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage())));
    // }
}
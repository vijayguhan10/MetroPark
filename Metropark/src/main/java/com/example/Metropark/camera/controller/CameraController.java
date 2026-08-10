package com.example.Metropark.camera.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.camera.dto.CameraDto;
import com.example.Metropark.camera.service.CameraService;

import jakarta.validation.Valid;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/cameras")
public class CameraController {

    private static final Logger LOGGER = LoggerFactory.getLogger(CameraController.class);

    private final CameraService service;

    public CameraController(CameraService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@Valid @RequestBody CameraDto dto) {
        LOGGER.info("Creating camera: {}", dto);
        return service.createCamera(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED)
                        .body("Camera created successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(e.getMessage())));
    }

    @GetMapping
    public Flux<CameraDto> getAll() {
        return service.getAllCameras();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<CameraDto>> getById(@PathVariable Integer id) {
        return service.getCameraById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @GetMapping("/location/{locationId}")
    public Flux<CameraDto> getByLocationId(@PathVariable String locationId) {
        return service.getCamerasByLocationId(locationId);
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Integer id, @Valid @RequestBody CameraDto dto) {
        LOGGER.info("Updating camera {}: {}", id, dto);
        return service.updateCamera(id, dto)
                .map(rows -> ResponseEntity.ok("Camera updated successfully."))
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())))
                .onErrorResume(IllegalStateException.class,
                        e -> Mono.just(ResponseEntity.status(HttpStatus.CONFLICT)
                                .body(e.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Integer id) {
        LOGGER.info("Deleting camera: {}", id);
        return service.deleteCamera(id)
                .map(rows -> rows > 0
                        ? ResponseEntity.ok("Camera deleted successfully.")
                        : ResponseEntity.notFound().build());
    }

    @PostMapping("/capture/entry")
    public Mono<ResponseEntity<String>> captureEntry(
            @RequestParam String licensePlate,
            @RequestParam String locationId,
            @RequestParam String cameraId) {
        LOGGER.info("Capturing entry event for license plate: {}", licensePlate);
        return service.captureEntryEvent(licensePlate, locationId, cameraId)
                .thenReturn(ResponseEntity.ok("Entry event captured successfully."))
                .onErrorResume(e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @PostMapping("/capture/exit")
    public Mono<ResponseEntity<String>> captureExit(
            @RequestParam String licensePlate,
            @RequestParam String locationId,
            @RequestParam String cameraId) {
        LOGGER.info("Capturing exit event for license plate: {}", licensePlate);
        return service.captureExitEvent(licensePlate, locationId, cameraId)
                .thenReturn(ResponseEntity.ok("Exit event captured successfully."))
                .onErrorResume(e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }
}
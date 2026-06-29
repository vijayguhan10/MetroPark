error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/VehicleTypeController.java:_empty_/ResponseEntity#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/VehicleTypeController.java
empty definition using pc, found symbol in pc: _empty_/ResponseEntity#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1257
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/VehicleTypeController.java
text:
```scala
package com.example.Metropark.controller;

import com.example.Metropark.dto.VehicleTypeDto;
import com.example.Metropark.service.VehicleTypeService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/vehicle-types")
public class VehicleTypeController {

    private final VehicleTypeService service;

    public VehicleTypeController(VehicleTypeService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody VehicleTypeDto dto) {
        return service.createVehicleType(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Vehicle type created successfully."))
                // Catch the validation error thrown by the Service layer
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<VehicleTypeDto> getAll() {
        return service.getAllVehicleTypes();
    }

    @GetMapping("/{id}")
    public Mono<Resp@@onseEntity<VehicleTypeDto>> getById(@PathVariable Integer id) {
        return service.getVehicleTypeById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Integer id, @RequestBody VehicleTypeDto dto) {
        return service.updateVehicleType(id, dto)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated > 0) {
                        return Mono.just(ResponseEntity.ok("Vehicle type updated successfully."));
                    } else {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                })
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Integer id) {
        return service.deleteVehicleType(id)
                .flatMap(rowsDeleted -> {
                    if (rowsDeleted > 0) {
                        return Mono.just(ResponseEntity.ok("Vehicle type deleted successfully."));
                    } else {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                });
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/ResponseEntity#
error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/LocationTypeController.java:_empty_/LocationTypeDto#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/LocationTypeController.java
empty definition using pc, found symbol in pc: _empty_/LocationTypeDto#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1101
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/LocationTypeController.java
text:
```scala
package com.example.Metropark.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.dto.LocationTypeDto;
import com.example.Metropark.service.LocationTypeService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/location-types")
public class LocationTypeController {

    private final LocationTypeService service;

    public LocationTypeController(LocationTypeService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<Lo@@cationTypeDto>> create(@RequestBody LocationTypeDto dto) {
        return service.createLocationType(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Location type created successfully.", rows:rows));
    }

    @GetMapping
    public Flux<LocationTypeDto> getAll() {
        return service.getAllLocationTypes();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<LocationTypeDto>> getById(@PathVariable Integer id) {
        return service.getLocationTypeById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Integer id, @RequestBody LocationTypeDto dto) {
        return service.updateLocationType(id, dto)
                .flatMap(rowsUpdated -> {
                    if (rowsUpdated > 0) {
                        return Mono.just(ResponseEntity.ok("Location type updated successfully."));
                    } else {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                });
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Integer id) {
        return service.deleteLocationType(id)
                .flatMap(rowsDeleted -> {
                    if (rowsDeleted > 0) {
                        return Mono.just(ResponseEntity.ok("Location type deleted successfully."));
                    } else {
                        return Mono.just(ResponseEntity.notFound().build());
                    }
                });
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/LocationTypeDto#
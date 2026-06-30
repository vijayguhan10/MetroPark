error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/ReservationClassController.java:java/lang/IllegalArgumentException#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/ReservationClassController.java
empty definition using pc, found symbol in pc: java/lang/IllegalArgumentException#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1930
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/ReservationClassController.java
text:
```scala
package com.example.Metropark.controller;

import com.example.Metropark.dto.ReservationClassDto;
import com.example.Metropark.service.ReservationClassService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/reservation-classes")
public class ReservationClassController {

    private final ReservationClassService service;

    public ReservationClassController(ReservationClassService service) {
        this.service = service;
    }

    @PostMapping
    public Mono<ResponseEntity<String>> create(@RequestBody ReservationClassDto dto) {
        return service.createReservationClass(dto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("Reservation class created successfully."))
                .onErrorResume(IllegalArgumentException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<ReservationClassDto> getAll() {
        return service.getAllReservationClasses();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<ReservationClassDto>> getById(@PathVariable Integer id) {
        return service.getReservationClassById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(@PathVariable Integer id, @RequestBody ReservationClassDto dto) {
        return service.updateReservationClass(id, dto)
                .flatMap(rows -> rows > 0 
                        ? Mono.just(ResponseEntity.ok("Reservation class updated successfully."))
                        : Mono.just(ResponseEntity.notFound().build()))
                .onErrorResume(IllegalArgum@@entException.class, e -> 
                        Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @DeleteMapping("/{id}")
    public Mono<ResponseEntity<String>> delete(@PathVariable Integer id) {
        return service.deleteReservationClass(id)
                .flatMap(rows -> rows > 0 
                        ? Mono.just(ResponseEntity.ok("Reservation class deleted successfully."))
                        : Mono.just(ResponseEntity.notFound().build()));
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: java/lang/IllegalArgumentException#
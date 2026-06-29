error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/VehicleTypeController.java
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/VehicleTypeController.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[74,2]

error in qdox parser
file content:
```java
offset: 2749
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
                .onErrorResume(IllegalArgumentException.class,
                        e -> Mono.just(ResponseEntity.badRequest().body(e.getMessage())));
    }

    @GetMapping
    public Flux<VehicleTypeDto> getAll() {
        return service.getAllVehicleTypes();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<VehicleTypeDto>> getById(@PathVariable Integer id) {
        return service.getVehicleTypeById(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    @PutMapping("/{id}")
    public Mono<ResponseEntity<String>> update(
            @PathVariable Integer id,
            @RequestBody VehicleTypeDto dto) {

       return service.updateVehicleType(id, dto)
        .flatMap(rowsUpdated -> {
            if (rowsUpdated > 0) {
                return Mono.just(
                        ResponseEntity.ok("Vehicle updated successfully"));
            } else {
                return Mono.just(
                        ResponseEntity.status(HttpStatus.NOT_FOUND)
                                .body("Vehicle not found"));
            }
        })
        .onErrorResume(ex ->
                Mono.just(
                        ResponseEntity.internalServerError()
                                .body("Internal Server Error")));

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
}@@
```

```



#### Error stacktrace:

```
com.thoughtworks.qdox.parser.impl.Parser.yyerror(Parser.java:2025)
	com.thoughtworks.qdox.parser.impl.Parser.yyparse(Parser.java:2147)
	com.thoughtworks.qdox.parser.impl.Parser.parse(Parser.java:2006)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:232)
	com.thoughtworks.qdox.library.SourceLibrary.parse(SourceLibrary.java:190)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:94)
	com.thoughtworks.qdox.library.SourceLibrary.addSource(SourceLibrary.java:89)
	com.thoughtworks.qdox.library.SortedClassLibraryBuilder.addSource(SortedClassLibraryBuilder.java:162)
	com.thoughtworks.qdox.JavaProjectBuilder.addSource(JavaProjectBuilder.java:174)
	scala.meta.internal.mtags.JavaMtags.indexRoot(JavaMtags.scala:49)
	scala.meta.internal.metals.SemanticdbDefinition$.foreachWithReturnMtags(SemanticdbDefinition.scala:99)
	scala.meta.internal.metals.Indexer.indexSourceFile(Indexer.scala:560)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3(Indexer.scala:691)
	scala.meta.internal.metals.Indexer.$anonfun$reindexWorkspaceSources$3$adapted(Indexer.scala:688)
	scala.collection.IterableOnceOps.foreach(IterableOnce.scala:630)
	scala.collection.IterableOnceOps.foreach$(IterableOnce.scala:628)
	scala.collection.AbstractIterator.foreach(Iterator.scala:1313)
	scala.meta.internal.metals.Indexer.reindexWorkspaceSources(Indexer.scala:688)
	scala.meta.internal.metals.MetalsLspService.$anonfun$onChange$2(MetalsLspService.scala:940)
	scala.runtime.java8.JFunction0$mcV$sp.apply(JFunction0$mcV$sp.scala:18)
	scala.concurrent.Future$.$anonfun$apply$1(Future.scala:691)
	scala.concurrent.impl.Promise$Transformation.run(Promise.scala:500)
	java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1144)
	java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:642)
	java.base/java.lang.Thread.run(Thread.java:1583)
```
#### Short summary: 

QDox parse error in file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/VehicleTypeController.java
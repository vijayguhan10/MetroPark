error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[1,1932]

error in qdox parser
file content:
```java
offset: 1931
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java
text:
```scala
package com.example.Metropark.service;  import com.example.Metropark.dto.VehicleTypeDto; import com.example.Metropark.repo.VehicleTypeRepository; import org.springframework.stereotype.Service; import reactor.core.publisher.Flux; import reactor.core.publisher.Mono;  @Service public class VehicleTypeService {      private final VehicleTypeRepository repository;      public VehicleTypeService(VehicleTypeRepository repository) {         this.repository = repository;     }      public Mono<Integer> createVehicleType(VehicleTypeDto dto) {         // 1. Validation: Prevent null or completely empty strings         if (dto.typeDisplayName() == null || dto.typeDisplayName().trim().isEmpty()) {             return Mono.error(new IllegalArgumentException("Vehicle type display name cannot be empty."));         }          // 2. Sanitization: Trim accidental leading/trailing whitespace         String cleanName = dto.typeDisplayName().trim();          // 3. Rebuild DTO with clean data         VehicleTypeDto cleanDto = new VehicleTypeDto(dto.vehicleTypeId(), cleanName);          return repository.create(cleanDto);     }      public Flux<VehicleTypeDto> getAllVehicleTypes() {         return repository.findAll();     }      public Mono<VehicleTypeDto> getVehicleTypeById(Integer id) {         return repository.findById(id);     }      public Mono<Integer> updateVehicleType(Integer id, VehicleTypeDto dto) {         if (dto.typeDisplayName() == null || dto.typeDisplayName().trim().isEmpty()) {             return Mono.error(new IllegalArgumentException("Vehicle type display name cannot be empty."));         }                  String cleanName = dto.typeDisplayName().trim();         VehicleTypeDto cleanDto = new VehicleTypeDto(dto.vehicleTypeId(), cleanName);                  return repository.update(id, cleanDto);     }      public Mono<Integer> deleteVehicleType(Integer id) {         return repository.delete(id);     } }@@
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

QDox parse error in file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java
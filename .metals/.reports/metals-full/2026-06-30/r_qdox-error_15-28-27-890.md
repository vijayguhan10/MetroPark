error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/x.java
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/x.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[1,2691]

error in qdox parser
file content:
```java
offset: 2690
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/x.java
text:
```scala
package com.example.Metropark.service;  import com.example.Metropark.dto.ReservationDto; import com.example.Metropark.repo.ReservationRepository; import org.springframework.stereotype.Service; import reactor.core.publisher.Flux; import reactor.core.publisher.Mono;  import java.time.LocalDateTime;  @Service public class ReservationService {      private final ReservationRepository repository;      public ReservationService(ReservationRepository repository) {         this.repository = repository;     }      public Mono<Integer> createReservation(ReservationDto dto) {         if (dto.userId() == null || dto.slotId() == null) {             return Mono.error(new IllegalArgumentException("User ID and Slot ID are strictly required."));         }          LocalDateTime now = LocalDateTime.now();                  // Business Rule: Auto-calculate expiry (e.g., 30 minutes from now) if not provided         LocalDateTime expiry = (dto.expiresAt() != null) ? dto.expiresAt() : now.plusMinutes(30);                  // Default to 'WAITING' if a queue entry is present, otherwise 'RESERVED'         String status = (dto.queueEntryId() != null) ? "WAITING" : "RESERVED";          ReservationDto processedDto = new ReservationDto(                 dto.reservationId(),                 dto.userId(),                 dto.slotId(),                 dto.queueEntryId(),                 status,                 1, // Base version starts at 1                 now, // Reserved at                 expiry,                 now, // Created at                 now  // Updated at         );          return repository.create(processedDto);     }      public Flux<ReservationDto> getAllReservations() {         return repository.findAll();     }      public Mono<ReservationDto> getReservationById(Integer id) {         return repository.findById(id);     }      // Handles the Concurrency Check     public Mono<Integer> updateStatus(Integer id, String status, Integer currentVersion) {         if (status == null || status.isBlank()) {             return Mono.error(new IllegalArgumentException("Status cannot be empty."));         }                  return repository.updateStatusWithOptimisticLock(id, status.trim().toUpperCase(), currentVersion)                 .flatMap(rowsUpdated -> {                     if (rowsUpdated == 0) {                         // If 0 rows updated, it means either the ID doesn't exist OR the version was wrong!                         return Mono.error(new IllegalStateException("Update failed: Concurrency conflict or Reservation not found. Please refresh and try again."));                     }                     return Mono.just(rowsUpdated);                 });     } }@@
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

QDox parse error in file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/x.java
error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/r.java
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/r.java
### com.thoughtworks.qdox.parser.ParseException: syntax error @[3,8]

error in qdox parser
file content:
```java
offset: 45
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/r.java
text:
```scala
package com.example.Metropark.repo;

public p@@ackage com.example.Metropark.repo;

import com.example.Metropark.dto.VehicleDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class VehicleRepository {

    private final DSLContext dsl;

    public VehicleRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(VehicleDto dto) {
        return Mono.from(dsl.insertInto(table("vehicles"))
                .columns(
                        field("user_id"), field("vehicle_number"), field("vehicle_type_id"),
                        field("brand"), field("model"), field("color"),
                        field("is_active"), field("created_at"), field("updated_at")
                )
                .values(
                        dto.userId(), dto.vehicleNumber(), dto.vehicleTypeId(),
                        dto.brand(), dto.model(), dto.color(),
                        dto.isActive(), dto.createdAt(), dto.updatedAt()
                ));
    }

    public Flux<VehicleDto> findAll() {
        return Flux.from(dsl.selectFrom(table("vehicles"))).map(this::mapToDto);
    }

    public Mono<VehicleDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("vehicles"))
                .where(field("vehicle_id").eq(id)))
                .map(this::mapToDto);
    }

    // Soft Delete / Toggle Active Status
    public Mono<Integer> updateActiveStatus(Integer id, Boolean isActive) {
        return Mono.from(dsl.update(table("vehicles"))
                .set(field("is_active"), isActive)
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("vehicle_id").eq(id)));
    }

    private VehicleDto mapToDto(Record record) {
        return new VehicleDto(
                record.get("vehicle_id", Integer.class),
                record.get("user_id", Integer.class),
                record.get("vehicle_number", String.class),
                record.get("vehicle_type_id", Integer.class),
                record.get("brand", String.class),
                record.get("model", String.class),
                record.get("color", String.class),
                record.get("is_active", Boolean.class),
                record.get("created_at", LocalDateTime.class),
                record.get("updated_at", LocalDateTime.class)
        );
    }
} {
    
}

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

QDox parse error in file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/r.java
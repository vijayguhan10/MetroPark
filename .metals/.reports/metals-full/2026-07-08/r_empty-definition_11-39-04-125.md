error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/gate/repo/GateRepository.java:_empty_/GateDto#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/gate/repo/GateRepository.java
empty definition using pc, found symbol in pc: _empty_/GateDto#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 562
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/gate/repo/GateRepository.java
text:
```scala
package com.example.Metropark.gate.repo;

import com.example.Metropark.gate.dto.GateDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class GateRepository {

    private final DSLContext dsl;

    public GateRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(G@@ateDto dto) {
        return Mono.from(dsl.insertInto(table("gates"))
                .columns(
                        field("location_id"), field("gate_name"), field("gate_type"),
                        field("status"), field("created_at"), field("updated_at")
                )
                .values(
                        dto.locationId(), dto.gateName(), dto.gateType(),
                        dto.status(), dto.createdAt(), dto.updatedAt()
                ));
    }

    public Flux<GateDto> findAll() {
        return Flux.from(dsl.selectFrom(table("gates"))).map(this::mapToDto);
    }

    public Mono<GateDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("gates"))
                .where(field("gate_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<Integer> updateStatus(Integer id, String status) {
        return Mono.from(dsl.update(table("gates"))
                .set(field("status"), status)
                .set(field("updated_at"), LocalDateTime.now())
                .where(field("gate_id").eq(id)));
    }

    private GateDto mapToDto(Record record) {
        return new GateDto(
                record.get("gate_id", Integer.class),
                record.get("location_id", String.class),
                record.get("gate_name", String.class),
                record.get("gate_type", String.class),
                record.get("status", String.class),
                record.get("created_at", LocalDateTime.class),
                record.get("updated_at", LocalDateTime.class)
        );
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/GateDto#
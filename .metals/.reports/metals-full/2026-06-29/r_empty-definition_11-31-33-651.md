error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/LocationTypeRepository.java:_empty_/Mono#from#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/LocationTypeRepository.java
empty definition using pc, found symbol in pc: _empty_/Mono#from#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1363
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/LocationTypeRepository.java
text:
```scala
package com.example.Metropark.repo;

import com.example.Metropark.dto.LocationTypeDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class LocationTypeRepository {

    private final DSLContext dsl;

    public LocationTypeRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    // CREATE: Omits type_id so the DB auto-increments it
    public Mono<Integer> create(LocationTypeDto dto) {
        return Mono.from(dsl.insertInto(table("location_types"))
                .columns(field("type_name"), field("Address"), field("Description"))
                .values(dto.typeName(), dto.address(), dto.description()));
    }

    // READ ALL
    public Flux<LocationTypeDto> findAll() {
        return Flux.from(dsl.selectFrom(table("location_types")))
                .map(this::mapToDto);
    }

    // READ BY ID
    public Mono<LocationTypeDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("location_types"))
                .where(field("type_id").eq(id)))
                .map(this::mapToDto);
    }

    // UPDATE
    public Mono<Integer> update(Integer id, LocationTypeDto dto) {
        return Mono.f@@rom(dsl.update(table("location_types"))
                .set(field("type_name"), dto.typeName())
                .set(field("Address"), dto.address())
                .set(field("Description"), dto.description())
                .where(field("type_id").eq(id)));
    }

    // DELETE
    public Mono<Integer> delete(Integer id) {
        return Mono.from(dsl.deleteFrom(table("location_types"))
                .where(field("type_id").eq(id)));
    }

    // MAPPER
    private LocationTypeDto mapToDto(Record record) {
        return new LocationTypeDto(
                record.get("type_id", Integer.class),
                record.get("type_name", String.class),
                record.get("Address", String.class),
                record.get("Description", String.class)
        );
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/Mono#from#
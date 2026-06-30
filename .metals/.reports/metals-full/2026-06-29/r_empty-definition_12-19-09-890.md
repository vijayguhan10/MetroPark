error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/LocationRepo.java:_empty_/LocationDto#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/LocationRepo.java
empty definition using pc, found symbol in pc: _empty_/LocationDto#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1730
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/LocationRepo.java
text:
```scala
package com.example.Metropark.repo;

import com.example.Metropark.dto.LocationDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class LocationRepository {

    private final DSLContext dsl;

    public LocationRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(LocationDto dto) {
        return Mono.from(dsl.insertInto(table("locations"))
                .columns(
                        field("location_id"), 
                        field("type_id"), 
                        field("location_name"), 
                        field("city"), 
                        field("status")
                )
                .values(
                        dto.locationId(), 
                        dto.typeId(), 
                        dto.locationName(), 
                        dto.city(), 
                        dto.status()
                ));
    }

    public Flux<LocationDto> findAll() {
        return Flux.from(dsl.selectFrom(table("locations")))
                .map(this::mapToDto);
    }

    public Mono<LocationDto> findById(String id) {
        return Mono.from(dsl.selectFrom(table("locations"))
                .where(field("location_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<Integer> updateStatus(String id, String status) {
        return Mono.from(dsl.update(table("locations"))
                .set(field("status"), status)
                .where(field("location_id").eq(id)));
    }

    private Location@@Dto mapToDto(Record record) {
        return new LocationDto(
                record.get("location_id", String.class),
                record.get("type_id", Integer.class),
                record.get("location_name", String.class),
                record.get("city", String.class),
                record.get("status", String.class)
        );
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/LocationDto#
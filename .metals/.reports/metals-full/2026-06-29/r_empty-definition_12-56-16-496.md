error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/VehicleTypeRepository.java:
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/VehicleTypeRepository.java
empty definition using pc, found symbol in pc: 
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 272
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/VehicleTypeRepository.java
text:
```scala
package com.example.Metropark.repo;

import com.example.Metropark.dto.VehicleTypeDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import stat@@ic org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class VehicleTypeRepository {

    private final DSLContext dsl;

    public VehicleTypeRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(VehicleTypeDto dto) {
        return Mono.from(dsl.insertInto(table("vehicle_types"))
                .columns(field("type_display_name"))
                .values(dto.typeDisplayName()));
    }

    public Flux<VehicleTypeDto> findAll() {
        return Flux.from(dsl.selectFrom(table("vehicle_types")))
                .map(this::mapToDto);
    }

    public Mono<VehicleTypeDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("vehicle_types"))
                .where(field("vehicle_type_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<Integer> update(Integer id, VehicleTypeDto dto) {
        return Mono.from(dsl.update(table("vehicle_types"))
                .set(field("type_display_name"), dto.typeDisplayName())
                .where(field("vehicle_type_id").eq(id)));
    }

    public Mono<Integer> delete(Integer id) {
        return Mono.from(dsl.deleteFrom(table("vehicle_types"))
                .where(field("vehicle_type_id").eq(id)));
    }

    private VehicleTypeDto mapToDto(Record record) {
        return new VehicleTypeDto(
                record.get("vehicle_type_id", Integer.class),
                record.get("type_display_name", String.class)
        );
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 
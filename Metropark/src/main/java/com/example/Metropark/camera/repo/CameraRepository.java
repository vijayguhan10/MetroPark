package com.example.Metropark.camera.repo;

import com.example.Metropark.camera.dto.CameraDto;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class CameraRepository {

    private final DSLContext dsl;

    public CameraRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<Integer> create(CameraDto dto) {
        return Mono.from(dsl.insertInto(table("cameras"))
                .columns(field("camera_name"), field("location_id"), field("camera_type"), field("status"),
                        field("created_at"), field("updated_at"))
                .values(dto.cameraName(), dto.locationId(), dto.cameraType(), dto.status(), dto.createdAt(),
                        dto.updatedAt()));
    }

    public Flux<CameraDto> findAll() {
        return Flux.from(dsl.selectFrom(table("cameras"))).map(this::mapToDto);
    }

    public Mono<CameraDto> findById(Integer id) {
        return Mono.from(dsl.selectFrom(table("cameras"))
                .where(field("camera_id").eq(id)))
                .map(this::mapToDto);
    }

    public Mono<CameraDto> findByCameraName(String cameraName) {
        return Mono.from(dsl.selectFrom(table("cameras"))
                .where(field("camera_name").eq(cameraName)))
                .map(this::mapToDto);
    }

    public Mono<Integer> update(Integer id, CameraDto dto) {
        return Mono.from(dsl.update(table("cameras"))
                .set(field("camera_name"), dto.cameraName())
                .set(field("location_id"), dto.locationId())
                .set(field("camera_type"), dto.cameraType())
                .set(field("status"), dto.status())
                .set(field("updated_at"), dto.updatedAt())
                .where(field("camera_id").eq(id)));
    }

    public Mono<Integer> delete(Integer id) {
        return Mono.from(dsl.deleteFrom(table("cameras"))
                .where(field("camera_id").eq(id)));
    }

    public Flux<CameraDto> findByLocationId(String locationId) {
        return Flux.from(dsl.selectFrom(table("cameras"))
                .where(field("location_id").eq(locationId)))
                .map(this::mapToDto);
    }

    private CameraDto mapToDto(Record record) {
        return new CameraDto(
                record.get("camera_id", Integer.class),
                record.get("camera_name", String.class),
                record.get("location_id", String.class),
                record.get("camera_type", String.class),
                record.get("status", String.class),
                record.get("created_at", java.time.LocalDateTime.class),
                record.get("updated_at", java.time.LocalDateTime.class));
    }
}

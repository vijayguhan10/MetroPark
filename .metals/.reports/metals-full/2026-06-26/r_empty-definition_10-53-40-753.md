error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/UserRepository.java:org/jooq/DSLContext#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/UserRepository.java
empty definition using pc, found symbol in pc: org/jooq/DSLContext#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 53
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/repo/UserRepository.java
text:
```scala
package com.example.Metropark.repo;

import org.jooq.@@DSLContext;
import org.jooq.Record;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import org.springframework.stereotype.Repository;

import com.example.Metropark.dto.UserDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
@Repository
public class UserRepository {

    private final DSLContext dsl;

    public UserRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Mono<UserDto> findById(String userId) {
        return Mono.from(dsl.selectFrom(table("users"))
                .where(field("user_id").eq(userId)))
                .map(this::mapToDto);
    }

    public Flux<UserDto> findAll() {
        return Flux.from(dsl.selectFrom(table("users")))
                .map(this::mapToDto);
    }

    public Mono<Integer> updateStatus(String userId, String status) {
        return Mono.from(dsl.update(table("users"))
                .set(field("user_status"), status)
                .where(field("user_id").eq(userId)));
    }

    private UserDto mapToDto(Record record) {
        return new UserDto(
                record.get("user_id", String.class),
                record.get("email", String.class),
                record.get("user_status", String.class)
        );
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: org/jooq/DSLContext#
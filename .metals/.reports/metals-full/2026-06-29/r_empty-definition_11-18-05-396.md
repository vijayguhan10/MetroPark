error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/LocationTypeService.java:_empty_/LocationTypeRepository#update#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/LocationTypeService.java
empty definition using pc, found symbol in pc: _empty_/LocationTypeRepository#update#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 917
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/LocationTypeService.java
text:
```scala
package com.example.Metropark.service;

import org.springframework.stereotype.Service;

import com.example.Metropark.dto.LocationTypeDto;
import com.example.Metropark.repo.LocationTypeRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class LocationTypeService {

    private final LocationTypeRepository repository;

    public LocationTypeService(LocationTypeRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createLocationType(LocationTypeDto dto) {
        return repository.create(dto);
    }

    public Flux<LocationTypeDto> getAllLocationTypes() {
        return repository.findAll();
    }

    public Mono<LocationTypeDto> getLocationTypeById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateLocationType(Integer id, LocationTypeDto dto) {
        return repository.up@@date(id, dto);
    }

    public Mono<Integer> deleteLocationType(Integer id) {
        return repository.delete(id);
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/LocationTypeRepository#update#
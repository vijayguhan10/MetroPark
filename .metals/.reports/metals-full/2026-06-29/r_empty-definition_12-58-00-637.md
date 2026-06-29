error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java:_empty_/VehicleTypeDto#typeDisplayName#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java
empty definition using pc, found symbol in pc: _empty_/VehicleTypeDto#typeDisplayName#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1458
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/VehicleTypeService.java
text:
```scala
package com.example.Metropark.service;

import com.example.Metropark.dto.VehicleTypeDto;
import com.example.Metropark.repo.VehicleTypeRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class VehicleTypeService {

    private final VehicleTypeRepository repository;

    public VehicleTypeService(VehicleTypeRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createVehicleType(VehicleTypeDto dto) {
        // 1. Validation: Prevent null or completely empty strings
        if (dto.typeDisplayName() == null || dto.typeDisplayName().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Vehicle type display name cannot be empty."));
        }

        // 2. Sanitization: Trim accidental leading/trailing whitespace
        String cleanName = dto.typeDisplayName().trim();

        // 3. Rebuild DTO with clean data
        VehicleTypeDto cleanDto = new VehicleTypeDto(dto.vehicleTypeId(), cleanName);

        return repository.create(cleanDto);
    }

    public Flux<VehicleTypeDto> getAllVehicleTypes() {
        return repository.findAll();
    }

    public Mono<VehicleTypeDto> getVehicleTypeById(Integer id) {
        return repository.findById(id);
    }

    public Mono<Integer> updateVehicleType(Integer id, VehicleTypeDto dto) {
        if (dto.typeDisplayName() == null || dto.@@typeDisplayName().trim().isEmpty()) {
            return Mono.error(new IllegalArgumentException("Vehicle type display name cannot be empty."));
        }
        
        String cleanName = dto.typeDisplayName().trim();
        VehicleTypeDto cleanDto = new VehicleTypeDto(dto.vehicleTypeId(), cleanName);
        
        return repository.update(id, cleanDto);
    }

    public Mono<Integer> deleteVehicleType(Integer id) {
        return repository.delete(id);
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/VehicleTypeDto#typeDisplayName#
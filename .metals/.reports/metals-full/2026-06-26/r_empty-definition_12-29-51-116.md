error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java:_empty_/SuspensionDto#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
empty definition using pc, found symbol in pc: _empty_/SuspensionDto#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 1132
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
text:
```scala
package com.parking.service;

import com.parking.dto.SuspensionDto;
import com.parking.dto.UserDto;
import com.parking.repository.SuspensionRepository;
import com.parking.repository.UserRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@Service
public class UserService {

    private final UserRepository userRepository;
    private final SuspensionRepository suspensionRepository;

    public UserService(UserRepository userRepository, SuspensionRepository suspensionRepository) {
        this.userRepository = userRepository;
        this.suspensionRepository = suspensionRepository;
    }

    public Mono<UserDto> getUser(String userId) {
        return userRepository.findById(userId);
    }

    public Flux<UserDto> getAllUsers() {
        return userRepository.findAll();
    }

    @Transactional
    public Mono<Void> suspendUser(String userId, String reason, LocalDateTime until, String adminId) {
        SuspensionDto suspension = new Sus@@pensionDto(
                null, userId, reason, LocalDateTime.now(), until, true, adminId
        );

        // Chain the reactive calls: Update user status THEN create suspension record
        return userRepository.updateStatus(userId, "SUSPENDED")
                .then(suspensionRepository.createSuspension(suspension))
                .then();
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/SuspensionDto#
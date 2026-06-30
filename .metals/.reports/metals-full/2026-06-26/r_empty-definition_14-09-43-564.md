error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java:java/util/Random#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
empty definition using pc, found symbol in pc: java/util/Random#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 274
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
text:
```scala
package com.example.Metropark.service;

import com.example.Metropark.dto.UserDto;
import com.example.Metropark.repo.UserRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
   import java.util.Ran@@dom;

@Service
public class UserService {

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }


public Mono<Integer> createUser(UserDto userDto) {
    // 1. Automatically generate an ID like "USR-8392"
    String generatedId = "USR-" + (new Random().nextInt(9000) + 1000); 

    // 2. Create a new DTO with the generated ID
    UserDto userToSave = new UserDto(
            generatedId, 
            userDto.email(), 
            userDto.userStatus() != null ? userDto.userStatus() : "ACTIVE"
    );

    // 3. Save to database
    return userRepository.createUser(userToSave);
}

    public Mono<UserDto> getUser(String userId) {
        return userRepository.findById(userId);
    }

    public Flux<UserDto> getAllUsers() {
        return userRepository.findAll();
    }

    public Mono<Integer> updateUserStatus(String userId, String status) {
        return userRepository.updateStatus(userId, status);
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: java/util/Random#
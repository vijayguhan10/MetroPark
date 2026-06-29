error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java:_empty_/Mono#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
empty definition using pc, found symbol in pc: _empty_/Mono#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 461
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/service/UserService.java
text:
```scala
package com.example.Metropark.service;

import com.example.Metropark.dto.UserDto;
import com.example.Metropark.repo.UserRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class UserService {

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public Mo@@no<Integer> createUser(UserDto userDto) {
        return userRepository.createUser(userDto);
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

empty definition using pc, found symbol in pc: _empty_/Mono#
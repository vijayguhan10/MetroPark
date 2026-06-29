error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/UserController.java:_empty_/UserService#getAllUsers#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/UserController.java
empty definition using pc, found symbol in pc: _empty_/UserService#getAllUsers#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 957
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/controller/UserController.java
text:
```scala
package com.example.Metropark.controller;

import com.example.Metropark.dto.UserDto;
import com.example.Metropark.service.UserService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/users")
public class UserController {

    private final UserService userService;

    public UserController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<ResponseEntity<String>> createUser(@RequestBody UserDto userDto) {
        return userService.createUser(userDto)
                .map(rows -> ResponseEntity.status(HttpStatus.CREATED).body("User created successfully."));
    }

    @GetMapping
    public Flux<UserDto> getAllUsers() {
        return userService.@@getAllUsers();
    }

    @GetMapping("/{id}")
    public Mono<ResponseEntity<UserDto>> getUserById(@PathVariable String id) {
        return userService.getUser(id)
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/UserService#getAllUsers#
package com.example.Metropark.user.service;

import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Random;

@Service
public class UserService {

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    public Mono<Integer> createUser(UserDto userDto) {
        String generatedId = "USR-" + (new Random().nextInt(9000) + 1000);

        UserDto userToSave = new UserDto(
                generatedId,
                userDto.email(),
                userDto.userStatus() != null ? userDto.userStatus() : "ACTIVE");

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
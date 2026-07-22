package com.example.Metropark.user.service;

import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Random;

@Service
public class UserService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UserService.class);

    private final UserRepository userRepository;

    public UserService(UserRepository userRepository) {
        this.userRepository = userRepository;
    }

    @Transactional
    public Mono<Integer> createUser(UserDto userDto) {
        LOGGER.info("Creating user: {}", userDto);
        String generatedId = "USR-" + (new Random().nextInt(9000) + 1000);

        UserDto userToSave = new UserDto(
                generatedId,
                userDto.name(),
                userDto.email(),
                userDto.phone(),
                userDto.userStatus() != null ? userDto.userStatus() : "ACTIVE",
                java.time.LocalDateTime.now());

        // 3. Save to database
        return userRepository.createUser(userToSave)
                .doOnSuccess(rows -> LOGGER.info("User created successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error creating user: {}", e.getMessage()));
    }

    public Mono<UserDto> getUser(String userId) {
        LOGGER.debug("Fetching user by id: {}", userId);
        return userRepository.findById(userId)
                .doOnSuccess(dto -> LOGGER.debug("Fetched user: {}", dto))
                .doOnError(e -> LOGGER.error("Error fetching user by id {}: {}", userId, e.getMessage()));
    }

    public Flux<UserDto> getAllUsers() {
        LOGGER.debug("Fetching all users");
        return userRepository.findAll()
                .doOnComplete(() -> LOGGER.debug("Fetched all users successfully"))
                .doOnError(e -> LOGGER.error("Error fetching all users: {}", e.getMessage()));
    }

    @Transactional
    public Mono<Integer> updateUserStatus(String userId, String status) {
        LOGGER.info("Updating user status id: {} to status: {}", userId, status);
        if (status == null || status.isBlank()) {
            return Mono.error(new IllegalArgumentException("Status cannot be empty."));
        }
        return userRepository.updateStatus(userId, status.trim().toUpperCase())
                .doOnSuccess(rows -> LOGGER.info("User status updated successfully, rows affected: {}", rows))
                .doOnError(e -> LOGGER.error("Error updating user status id {}: {}", userId, e.getMessage()));
    }
}
package com.example.Metropark.service;

import com.example.Metropark.dto.SuspensionDto;
import com.example.Metropark.dto.UserDto;
import com.example.Metropark.repo.SuspensionRepository;
import com.example.Metropark.repo.UserRepository;
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
        SuspensionDto suspension = new SuspensionDto(
                null, userId, reason, LocalDateTime.now(), until, true, adminId
        );

        // Chain the reactive calls: Update user status THEN create suspension record
        return userRepository.updateStatus(userId, "SUSPENDED")
                .then(suspensionRepository.createSuspension(suspension))
                .then();
    }
}
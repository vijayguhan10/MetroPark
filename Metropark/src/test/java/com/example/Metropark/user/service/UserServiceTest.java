package com.example.Metropark.user.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;

import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class UserServiceTest {

    @Mock
    private UserRepository userRepository;

    @InjectMocks
    private UserService service;

    @Test
    void createUserGeneratesIdAndDefaultsStatus() {
        UserDto input = new UserDto(null, "user@example.com", null);
        when(userRepository.createUser(any())).thenReturn(Mono.just(1));

        Integer rows = service.createUser(input).block();

        assertEquals(1, rows);
        ArgumentCaptor<UserDto> captor = ArgumentCaptor.forClass(UserDto.class);
        verify(userRepository).createUser(captor.capture());

        UserDto saved = captor.getValue();
        assertTrue(saved.userId().matches("USR-\\d{4}"));
        assertEquals("user@example.com", saved.email());
        assertEquals("ACTIVE", saved.userStatus());
    }

    @Test
    void updateUserStatusReturnsRows() {
        when(userRepository.updateStatus("USR-1001", "SUSPENDED")).thenReturn(Mono.just(1));

        Integer rows = service.updateUserStatus("USR-1001", " suspended ").block();

        assertEquals(1, rows);
        verify(userRepository).updateStatus("USR-1001", "SUSPENDED");
    }
}

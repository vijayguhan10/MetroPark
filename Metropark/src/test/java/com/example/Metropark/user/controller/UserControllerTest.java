package com.example.Metropark.user.controller;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.reactive.server.WebTestClient;

import com.example.Metropark.testsupport.TestFixtures;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.service.UserService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@ExtendWith(MockitoExtension.class)
class UserControllerTest {

    @Mock
    private UserService userService;

    private WebTestClient webTestClient;

    @BeforeEach
    void setUp() {
        webTestClient = WebTestClient.bindToController(new UserController(userService)).build();
    }

    @Test
    void createUserReturnsCreatedMessage() {
        when(userService.createUser(any())).thenReturn(Mono.just(1));

        String response = webTestClient.post()
                .uri("/api/users")
                .bodyValue(TestFixtures.userDto())
                .exchange()
                .expectStatus().isCreated()
                .expectBody(String.class)
                .returnResult()
                .getResponseBody();

        assertEquals("User created successfully.", response);
    }

    @Test
    void getAllUsersReturnsList() {
        when(userService.getAllUsers()).thenReturn(Flux.just(TestFixtures.userDto()));

        List<UserDto> response = webTestClient.get()
                .uri("/api/users")
                .exchange()
                .expectStatus().isOk()
                .expectBodyList(UserDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(List.of(TestFixtures.userDto()), response);
    }

    @Test
    void getUserByIdReturnsUser() {
        when(userService.getUser("USR-1001")).thenReturn(Mono.just(TestFixtures.userDto()));

        UserDto response = webTestClient.get()
                .uri("/api/users/USR-1001")
                .exchange()
                .expectStatus().isOk()
                .expectBody(UserDto.class)
                .returnResult()
                .getResponseBody();

        assertEquals(TestFixtures.userDto(), response);
    }
}

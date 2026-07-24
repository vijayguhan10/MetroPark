package com.example.Metropark.user.dto;

import java.time.LocalDateTime;

public record UserDto(
    String userId,
    String name,
    String email,
    String phone,
    String userStatus,
    LocalDateTime createdAt
) {}

package com.example.Metropark.user.dto;


import java.time.LocalDateTime;

public record SuspensionDto(
    Integer suspensionId,
    String userId,
    String reason,
    LocalDateTime suspendedAt,
    LocalDateTime suspendedUntil,
    Boolean isActive,
    String authorizedBy
) {}
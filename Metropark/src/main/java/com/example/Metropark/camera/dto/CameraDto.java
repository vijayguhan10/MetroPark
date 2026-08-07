package com.example.Metropark.camera.dto;

import java.time.LocalDateTime;

public record CameraDto(
        Integer cameraId,
        String cameraName,
        String locationId,
        String cameraType,
        String status,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
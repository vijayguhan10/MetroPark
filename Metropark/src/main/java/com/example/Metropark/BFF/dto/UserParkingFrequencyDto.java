package com.example.Metropark.BFF.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record UserParkingFrequencyDto(
        String userId,
        String name,
        String email,
        String phone,
        int totalSessions,
        int totalDurationMinutes,
        BigDecimal totalSpent,
        LocalDateTime lastParked,
        LocalDateTime joinedDate
) {
    public record UserParkingFrequencySummaryDto(
            String userId,
            String name,
            String email,
            String phone,
            int totalSessions,
            int totalDurationMinutes,
            BigDecimal totalSpent,
            LocalDateTime lastParked
    ) {}

    public record AllUsersDetailDto(
            String userId,
            String name,
            String email,
            String phone,
            LocalDateTime joinedDate
    ) {}

    public record UserParkingFrequencyResponse(
            List<UserParkingFrequencySummaryDto> sessions,
            List<AllUsersDetailDto> allUsersDetail
    ) {}
}
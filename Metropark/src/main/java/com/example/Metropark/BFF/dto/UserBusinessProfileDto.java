package com.example.Metropark.BFF.dto;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record UserBusinessProfileDto(
                String userId,
                String name,
                String email,
                String phone,
                LocalDateTime joinedDate,

                // Aggregated Usage Data
                int totalSessions,
                int totalDurationMinutes,
                BigDecimal totalLifetimeValue, // Total money they have spent
                LocalDateTime lastParked,

                // Calculated Business Insights (Calculated in the Service)
                BigDecimal averageSpendPerSession,
                String customerSegment // "VIP", "REGULAR", "AT_RISK", "INACTIVE"
) {
}
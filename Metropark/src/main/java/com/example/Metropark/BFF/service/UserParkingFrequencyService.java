package com.example.Metropark.BFF.service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

import org.jooq.DSLContext;
import static org.jooq.impl.DSL.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.BFF.dto.UserBusinessProfileDto;

import reactor.core.publisher.Flux;

@Service
public class UserParkingFrequencyService {

        private static final Logger LOGGER = LoggerFactory.getLogger(UserParkingFrequencyService.class);
        private final DSLContext dsl;

        public UserParkingFrequencyService(DSLContext dsl) {
                this.dsl = dsl;
        }

        public Flux<UserBusinessProfileDto> getUltraRichUserInsights() {
                LOGGER.info("Fetching ultra-rich user business profiles...");

                var query = dsl.select(
                                field("u.user_id", String.class).as("userId"),
                                field("u.name", String.class).as("name"),
                                field("u.email", String.class).as("email"),
                                field("u.phone", String.class).as("phone"),
                                field("u.created_at", LocalDateTime.class).as("joinedDate"),

                                countDistinct(field("ps.session_id")).as("totalSessions"),
                                sum(field("ps.duration_minutes", Integer.class)).as("totalDurationMinutes"),
                                sum(field("p.amount", BigDecimal.class)).as("totalSpent"),
                                max(field("ps.actual_entry_time", LocalDateTime.class)).as("lastParked"))
                                .from(table("users").as("u"))

                                .leftJoin(table("parking_sessions").as("ps"))
                                .on(field("u.user_id").eq(field("ps.user_id")))
                                .leftJoin(table("payments").as("p"))
                                .on(field("ps.session_id").eq(field("p.session_id")))
                                .and(field("p.payment_status").eq("SUCCESS"))
                                .groupBy(
                                                field("u.user_id"),
                                                field("u.name"),
                                                field("u.email"),
                                                field("u.phone"),
                                                field("u.created_at"))

                                .orderBy(sum(field("p.amount", BigDecimal.class)).desc().nullsLast());

                return Flux.from(query).map(record -> {

                        int sessions = record.get("totalSessions", Integer.class) != null
                                        ? record.get("totalSessions", Integer.class)
                                        : 0;
                        int duration = record.get("totalDurationMinutes", Integer.class) != null
                                        ? record.get("totalDurationMinutes", Integer.class)
                                        : 0;
                        BigDecimal totalSpent = record.get("totalSpent", BigDecimal.class) != null
                                        ? record.get("totalSpent", BigDecimal.class)
                                        : BigDecimal.ZERO;
                        LocalDateTime lastParked = record.get("lastParked", LocalDateTime.class);

                        BigDecimal avgSpend = sessions > 0
                                        ? totalSpent.divide(new BigDecimal(sessions), 2, RoundingMode.HALF_UP)
                                        : BigDecimal.ZERO;

                        String segment = calculateCustomerSegment(sessions, totalSpent, lastParked);

                        return new UserBusinessProfileDto(
                                        record.get("userId", String.class),
                                        record.get("name", String.class),
                                        record.get("email", String.class),
                                        record.get("phone", String.class),
                                        record.get("joinedDate", LocalDateTime.class),
                                        sessions,
                                        duration,
                                        totalSpent,
                                        lastParked,
                                        avgSpend,
                                        segment);
                });
        }

        
        private String calculateCustomerSegment(int sessions, BigDecimal totalSpent, LocalDateTime lastParked) {
                if (sessions == 0) {
                        return "INACTIVE_NEW";
                }

                long daysSinceLastPark = ChronoUnit.DAYS.between(lastParked, LocalDateTime.now());

                if (totalSpent.compareTo(new BigDecimal("500.00")) > 0 && daysSinceLastPark < 30) {
                        return "VIP_CUSTOMER";
                }

                if (daysSinceLastPark > 60) {
                        return "CHURN_RISK";
                }

                return "REGULAR_ACTIVE";
        }
}

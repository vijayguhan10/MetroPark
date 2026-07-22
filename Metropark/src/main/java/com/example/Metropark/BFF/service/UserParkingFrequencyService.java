package com.example.Metropark.BFF.service;

import com.example.Metropark.BFF.dto.UserParkingFrequencyDto;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.table;

@Service
public class UserParkingFrequencyService {

        private static final Logger LOGGER = LoggerFactory.getLogger(UserParkingFrequencyService.class);

        private final DSLContext dsl;

        public UserParkingFrequencyService(DSLContext dsl) {
                this.dsl = dsl;
        }

        public Mono<UserParkingFrequencyDto.UserParkingFrequencyResponse> getUserParkingFrequency() {
                LOGGER.info("Fetching user parking frequency data");

                return Mono.zip(
                                getUserParkingSessions(),
                                getAllUsersDetail())
                                .map(tuple -> new UserParkingFrequencyDto.UserParkingFrequencyResponse(
                                                tuple.getT1(),
                                                tuple.getT2()));
        }

        public Mono<List<UserParkingFrequencyDto.UserParkingFrequencySummaryDto>> getUserParkingSessions() {
                return Mono.from(dsl.select(
                                field("u.user_id").as("userId"),
                                field("u.name").as("name"),
                                field("u.email").as("email"),
                                field("u.phone").as("phone"),
                                count(field("ps.session_id")).as("totalSessions"),
                                sum(field("ps.duration_minutes", Integer.class)).as("totalDurationMinutes"),
                                sum(field("p.amount", BigDecimal.class)).as("totalSpent"),
                                field("MAX(ps.actual_entry_time)").as("lastParked"))
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
                                                field("u.phone"))
                                .orderBy(field("lastParked").desc()))
                                .map(records -> records.map(
                                                record -> new UserParkingFrequencyDto.UserParkingFrequencySummaryDto(
                                                                record.get("userId", String.class),
                                                                record.get("name", String.class),
                                                                record.get("email", String.class),
                                                                record.get("phone", String.class),
                                                                record.get("totalSessions", Integer.class) != null
                                                                                ? record.get("totalSessions", Integer.class)
                                                                                : 0,
                                                                record.get("totalDurationMinutes", Integer.class) != null
                                                                                ? record.get("totalDurationMinutes", Integer.class)
                                                                                : 0,
                                                                record.get("totalSpent", BigDecimal.class) != null
                                                                                ? record.get("totalSpent", BigDecimal.class)
                                                                                : BigDecimal.ZERO,
                                                                record.get("lastParked", LocalDateTime.class)))
                                                .stream().toList())
                                .defaultIfEmpty(List.of())
                                .onErrorReturn(List.of());
        }

        public Mono<List<UserParkingFrequencyDto.AllUsersDetailDto>> getAllUsersDetail() {
                return Mono.from(dsl.select(
                                field("u.user_id").as("userId"),
                                field("u.name").as("name"),
                                field("u.email").as("email"),
                                field("u.phone").as("phone"),
                                field("u.created_at").as("joinedDate"))
                                .from(table("users").as("u"))
                                .orderBy(field("u.created_at").desc()))
                                .map(records -> records.map(record -> new UserParkingFrequencyDto.AllUsersDetailDto(
                                                record.get("userId", String.class),
                                                record.get("name", String.class),
                                                record.get("email", String.class),
                                                record.get("phone", String.class),
                                                record.get("joinedDate", LocalDateTime.class))).stream().toList())
                                .defaultIfEmpty(List.of())
                                .onErrorReturn(List.of());
        }
}
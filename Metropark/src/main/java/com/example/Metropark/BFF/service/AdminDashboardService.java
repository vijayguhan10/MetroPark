package com.example.Metropark.BFF.service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import org.jooq.DSLContext;
import static org.jooq.impl.DSL.count;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.sum;
import static org.jooq.impl.DSL.table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.BFF.dto.ActiveSessionDto;
import com.example.Metropark.BFF.dto.AdminDashboardDto;
import com.example.Metropark.BFF.dto.AlertDto;
import com.example.Metropark.BFF.dto.DashboardSummaryDto;
import com.example.Metropark.BFF.dto.GateStatusDto;
import com.example.Metropark.BFF.dto.OccupancyByLocationDto;
import com.example.Metropark.BFF.dto.RevenueDto;
import com.example.Metropark.gate.repo.GateRepository;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.parking.repo.ParkingSessionRepository;
import com.example.Metropark.parking.repo.ParkingSlotRepository;

import reactor.core.publisher.Mono;

@Service
public class AdminDashboardService {

        private static final Logger LOGGER = LoggerFactory.getLogger(AdminDashboardService.class);

        private final ParkingSessionRepository sessionRepository;
        private final ParkingSlotRepository slotRepository;
        private final GateRepository gateRepository;
        private final LocationRepository locationRepository;
        private final DSLContext dsl;

        public AdminDashboardService(
                        ParkingSessionRepository sessionRepository,
                        ParkingSlotRepository slotRepository,
                        GateRepository gateRepository,
                        LocationRepository locationRepository,
                        DSLContext dsl) {
                this.sessionRepository = sessionRepository;
                this.slotRepository = slotRepository;
                this.gateRepository = gateRepository;
                this.locationRepository = locationRepository;
                this.dsl = dsl;
        }

        public Mono<AdminDashboardDto> getDashboardData() {
                LOGGER.info("Fetching admin dashboard data");

                return Mono.zip(
                                getSummary(),
                                getActiveSessions(),
                                getOccupancyByLocation(),
                                getAlerts(),
                                getRevenueToday(),
                                getGateStatuses()).map(
                                                tuple -> new AdminDashboardDto(
                                                                tuple.getT1(),
                                                                tuple.getT2(),
                                                                tuple.getT3(),
                                                                tuple.getT4(),
                                                                tuple.getT5(),
                                                                tuple.getT6(),
                                                                LocalDateTime.now()));
        }

        private Mono<DashboardSummaryDto> getSummary() {
                return Mono.zip(
                                slotRepository.findAll().count(),
                                slotRepository.findAll()
                                                .filter(slot -> "OCCUPIED".equalsIgnoreCase(slot.currentStatus()))
                                                .count(),
                                sessionRepository.findAll()
                                                .filter(session -> "ACTIVE".equalsIgnoreCase(session.sessionStatus())
                                                                || "CREATED".equalsIgnoreCase(session.sessionStatus()))
                                                .count(),
                                getOpenAlertsCount()).map(tuple -> {
                                        long totalSlots = tuple.getT1();
                                        long occupiedSlots = tuple.getT2();
                                        long activeSessions = tuple.getT3();
                                        int openAlerts = tuple.getT4();
                                        long availableSlots = totalSlots - occupiedSlots;
                                        double occupancyPercentage = totalSlots > 0
                                                        ? (double) occupiedSlots / totalSlots * 100
                                                        : 0.0;

                                        return new DashboardSummaryDto(
                                                        activeSessions,
                                                        Math.round(occupancyPercentage * 100.0) / 100.0,
                                                        openAlerts,
                                                        totalSlots,
                                                        occupiedSlots,
                                                        availableSlots);
                                });
        }

        private Mono<List<ActiveSessionDto>> getActiveSessions() {
                return sessionRepository.findAll()
                                .filter(session -> "ACTIVE".equalsIgnoreCase(session.sessionStatus())
                                                || "CREATED".equalsIgnoreCase(session.sessionStatus()))
                                .flatMap(session -> enrichSession(session))
                                .collectList();
        }

        private Mono<ActiveSessionDto> enrichSession(com.example.Metropark.parking.dto.ParkingSessionDto session) {
                return Mono.zip(
                                getVehiclePlate(session.vehicleId()),
                                getSlotDisplayCode(session.slotId()),
                                getVehicleDisplayName(session.vehicleId())).map(
                                                tuple -> new ActiveSessionDto(
                                                                "#SN-" + session.sessionId(),
                                                                tuple.getT1(),
                                                                tuple.getT3(),
                                                                tuple.getT2(),
                                                                session.sessionStatus(),
                                                                session.actualEntryTime(),
                                                                session.durationMinutes()));
        }

        private Mono<String> getVehiclePlate(Integer vehicleId) {
                if (vehicleId == null)
                        return Mono.just("N/A");
                return Mono.from(dsl.selectFrom(table("vehicles"))
                                .where(field("vehicle_id").eq(vehicleId)))
                                .map(record -> record.get("license_plate", String.class))
                                .defaultIfEmpty("N/A");
        }

        private Mono<String> getSlotDisplayCode(Integer slotId) {
                if (slotId == null)
                        return Mono.just("N/A");
                return Mono.from(dsl.selectFrom(table("parking_slots"))
                                .where(field("slot_id").eq(slotId)))
                                .map(record -> record.get("display_code", String.class))
                                .defaultIfEmpty("N/A");
        }

        private Mono<String> getVehicleDisplayName(Integer vehicleId) {
                if (vehicleId == null)
                        return Mono.just("Unknown");
                return Mono.from(dsl.selectFrom(table("vehicles"))
                                .where(field("vehicle_id").eq(vehicleId)))
                                .flatMap(record -> {
                                        String make = record.get("make", String.class);
                                        String model = record.get("model", String.class);
                                        return Mono.just(
                                                        (make != null ? make : "") + " " + (model != null ? model : ""))
                                                        .map(value -> value.trim());
                                })
                                .defaultIfEmpty("Unknown");
        }

        private Mono<List<OccupancyByLocationDto>> getOccupancyByLocation() {
                return locationRepository.findAll()
                                .flatMap(location -> Mono.zip(
                                                Mono.just(location),
                                                slotRepository.findAll()
                                                                .filter(slot -> location.locationId()
                                                                                .equals(slot.locationId()))
                                                                .count(),
                                                slotRepository.findAll()
                                                                .filter(slot -> location.locationId()
                                                                                .equals(slot.locationId())
                                                                                && "OCCUPIED".equalsIgnoreCase(
                                                                                                slot.currentStatus()))
                                                                .count())
                                                .map(tuple -> {
                                                        var loc = tuple.getT1();
                                                        long total = tuple.getT2();
                                                        long occupied = tuple.getT3();
                                                        long available = total - occupied;
                                                        double percentage = total > 0 ? (double) occupied / total * 100
                                                                        : 0.0;

                                                        return new OccupancyByLocationDto(
                                                                        loc.locationId(),
                                                                        loc.locationName(),
                                                                        total,
                                                                        occupied,
                                                                        available,
                                                                        Math.round(percentage * 100.0) / 100.0);
                                                }))
                                .collectList();
        }

        private Mono<List<AlertDto>> getAlerts() {
                // For now, return mock alerts since there's no alerts table
                // In production, this would query an alerts table
                return Mono.just(List.of(
                                new AlertDto(
                                                "ALT-001",
                                                "OCCUPANCY_HIGH",
                                                "WARNING",
                                                "Location LOC-CENTRAL-HUB at 85% occupancy",
                                                "LOC-CENTRAL-HUB",
                                                LocalDateTime.now().minusMinutes(15),
                                                false),
                                new AlertDto(
                                                "ALT-002",
                                                "GATE_OFFLINE",
                                                "CRITICAL",
                                                "Exit Gate B at LOC-CENTRAL-HUB is offline",
                                                "LOC-CENTRAL-HUB",
                                                LocalDateTime.now().minusMinutes(5),
                                                false),
                                new AlertDto(
                                                "ALT-003",
                                                "PAYMENT_FAILED",
                                                "WARNING",
                                                "3 payments failed in last hour at LOC-EVENT-GROUNDS",
                                                "LOC-EVENT-GROUNDS",
                                                LocalDateTime.now().minusMinutes(30),
                                                false)));
        }

        private Mono<Integer> getOpenAlertsCount() {
                return getAlerts().map(alerts -> alerts.size());
        }

        private Mono<RevenueDto> getRevenueToday() {
                LocalDateTime startOfDay = LocalDate.now().atStartOfDay();
                LocalDateTime endOfDay = LocalDate.now().atTime(LocalTime.MAX);

                return Mono.from(dsl.select(
                                sum(field("amount", BigDecimal.class)).as("total_amount"),
                                count().as("transaction_count"))
                                .from(table("payments"))
                                .where(field("processed_at").between(startOfDay, endOfDay))
                                .and(field("payment_status").eq("SUCCESS")))
                                .map(record -> {
                                        BigDecimal totalAmount = record.get("total_amount", BigDecimal.class);
                                        Long transactionCount = record.get("transaction_count", Long.class);

                                        return new RevenueDto(
                                                        totalAmount != null ? totalAmount : BigDecimal.ZERO,
                                                        "INR",
                                                        transactionCount != null ? transactionCount : 0L,
                                                        "TODAY");
                                })
                                .defaultIfEmpty(new RevenueDto(BigDecimal.ZERO, "INR", 0L, "TODAY"))
                                .onErrorReturn(new RevenueDto(BigDecimal.ZERO, "INR", 0L, "TODAY"));
        }

        private Mono<List<GateStatusDto>> getGateStatuses() {
                return gateRepository.findAll()
                                .map(gate -> new GateStatusDto(
                                                gate.gateId(),
                                                gate.gateName(),
                                                gate.gateType(),
                                                gate.status(),
                                                gate.locationId()))
                                .collectList();
        }
}
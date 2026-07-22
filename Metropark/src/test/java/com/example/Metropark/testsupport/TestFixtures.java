package com.example.Metropark.testsupport;

import java.math.BigDecimal;
import java.time.LocalDateTime;

import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.dto.LocationTypeDto;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.dto.PricingRateDto;
import com.example.Metropark.queue.dto.QueueDto;
import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.dto.ReservationDto;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.dto.VehicleTypeDto;

public final class TestFixtures {

    private static final LocalDateTime FIXED_TIME = LocalDateTime.of(2024, 1, 15, 10, 30);

    private TestFixtures() {
    }

    public static VehicleDto vehicleDto() {
        return new VehicleDto(1, "USR-1001", "ABC123", 2, "Toyota", "Corolla", "Blue", true, FIXED_TIME, FIXED_TIME);
    }

    public static VehicleTypeDto vehicleTypeDto() {
        return new VehicleTypeDto(1, "Sedan");
    }

    public static GateDto gateDto() {
        return new GateDto(1, "LOC-1", "North Gate", "ENTRY", "ACTIVE", FIXED_TIME, FIXED_TIME);
    }

    public static QueueDto queueDto() {
        return new QueueDto(1, "LOC-1", 2, 3, "Main Queue", "ACTIVE", FIXED_TIME);
    }

    public static ParkingSlotDto parkingSlotDto() {
        return new ParkingSlotDto(1, "LOC-1", "A-01", 2, 3, "SENSOR-1", "AVAILABLE");
    }

    public static ParkingSessionDto parkingSessionDto() {
        return new ParkingSessionDto(
                1,
                10,
                5,
                "USR-1001",
                20,
                3,
                null,
                "CREATED",
                FIXED_TIME,
                null,
                FIXED_TIME.plusHours(2),
                120,
                "PENDING",
                1,
                FIXED_TIME,
                FIXED_TIME);
    }

    public static ReservationDto reservationDto() {
        return new ReservationDto(1, 1001, 5, null, "RESERVED", 1, FIXED_TIME, FIXED_TIME.plusMinutes(30), FIXED_TIME, FIXED_TIME);
    }

    public static ReservationClassDto reservationClassDto() {
        return new ReservationClassDto(1, "Vip");
    }

    public static LocationDto locationDto() {
        return new LocationDto("LOC-1", 1, "Central Lot", "Metro City", "ACTIVE");
    }

    public static LocationTypeDto locationTypeDto() {
        return new LocationTypeDto(1L, "Campus");
    }

    public static UserDto userDto() {
        return new UserDto("USR-1001", "Test User", "user@example.com", "+91-9876543210", "ACTIVE", java.time.LocalDateTime.of(2024, 1, 15, 10, 30));
    }

    public static UserDto rajeshKumarUserDto() {
        return new UserDto("USR-001", "Rajesh Kumar", "rajesh.kumar@email.com", "+91-9876543210", "ACTIVE", java.time.LocalDateTime.of(2023, 5, 10, 0, 0));
    }

    public static PaymentMethodDto paymentMethodDto() {
        return new PaymentMethodDto(1L, "CREDIT_CARD", true, FIXED_TIME);
    }

    public static PaymentDto paymentDto() {
        return new PaymentDto(
                1L,
                "TX-100",
                10,
                "USR-1001",
                1,
                new BigDecimal("15.50"),
                "USD",
                "PENDING",
                null,
                null,
                null,
                FIXED_TIME,
                FIXED_TIME);
    }

    public static PaymentStatusUpdateDto paymentStatusUpdateDto() {
        return new PaymentStatusUpdateDto("SUCCESS", "ADMIN", "Approved", "GW-1");
    }

    public static PricingRateDto pricingRateDto() {
        return new PricingRateDto(
                1L,
                "LOC-1",
                2,
                new BigDecimal("10.00"),
                "USD",
                FIXED_TIME,
                null,
                true,
                FIXED_TIME,
                FIXED_TIME);
    }
}

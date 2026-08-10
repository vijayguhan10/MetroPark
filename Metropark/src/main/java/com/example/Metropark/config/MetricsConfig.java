package com.example.Metropark.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Custom business metrics for the MetroPark application.
 *
 * <p>These are exported to Prometheus through {@code /actuator/prometheus} and
 * drive the "MetroPark Overview" Grafana dashboard.
 */
@Configuration
public class MetricsConfig {

    private final MeterRegistry meterRegistry;

    public MetricsConfig(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    /**
     * Custom counter for parking session events.
     *
     * <p>Named "opened" rather than "created": the Prometheus client treats
     * {@code _created} as a reserved suffix and strips it, so a meter called
     * {@code metropark.parking.session.created} would be published as
     * {@code metropark_parking_session_total} — which reads as a total count of
     * sessions rather than a count of creations. The same applies to
     * {@link #reservationBookedCounter()} and {@link #slotRegisteredCounter()}.
     */
    @Bean
    public Counter parkingSessionCreatedCounter() {
        return Counter.builder("metropark.parking.session.opened")
                .description("Number of parking sessions created")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    @Bean
    public Counter parkingSessionEndedCounter() {
        return Counter.builder("metropark.parking.session.ended")
                .description("Number of parking sessions ended")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    @Bean
    public Counter parkingSessionStatusChangedCounter() {
        return Counter.builder("metropark.parking.session.status.changed")
                .description("Number of parking session status changes")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    /**
     * Custom counter for reservation events.
     */
    @Bean
    public Counter reservationBookedCounter() {
        return Counter.builder("metropark.reservation.booked")
                .description("Number of reservations created")
                .tag("service", "reservation")
                .register(meterRegistry);
    }

    @Bean
    public Counter reservationCancelledCounter() {
        return Counter.builder("metropark.reservation.cancelled")
                .description("Number of reservations cancelled")
                .tag("service", "reservation")
                .register(meterRegistry);
    }

    /**
     * Custom counter for slot events.
     */
    @Bean
    public Counter slotRegisteredCounter() {
        return Counter.builder("metropark.slot.registered")
                .description("Number of parking slots created")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    @Bean
    public Counter slotUpdatedCounter() {
        return Counter.builder("metropark.slot.updated")
                .description("Number of parking slots updated")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    /**
     * Custom counter for payment events.
     */
    @Bean
    public Counter paymentCompletedCounter() {
        return Counter.builder("metropark.payment.completed")
                .description("Number of payments completed")
                .tag("service", "payments")
                .register(meterRegistry);
    }

    @Bean
    public Counter paymentFailedCounter() {
        return Counter.builder("metropark.payment.failed")
                .description("Number of payments failed")
                .tag("service", "payments")
                .register(meterRegistry);
    }

    /**
     * Custom timer for database operations.
     */
    @Bean
    public Timer databaseOperationTimer() {
        return Timer.builder("metropark.db.operation.duration")
                .description("Database operation duration")
                .tag("service", "database")
                .register(meterRegistry);
    }

    /**
     * Custom timer for RabbitMQ publish operations.
     */
    @Bean
    public Timer rabbitmqPublishTimer() {
        return Timer.builder("metropark.rabbitmq.publish.duration")
                .description("RabbitMQ publish duration")
                .tag("service", "messaging")
                .register(meterRegistry);
    }

    /**
     * Custom timer for Redis operations.
     */
    @Bean
    public Timer redisOperationTimer() {
        return Timer.builder("metropark.redis.operation.duration")
                .description("Redis operation duration")
                .tag("service", "cache")
                .register(meterRegistry);
    }

    /**
     * Custom counter for camera events.
     */
    @Bean
    public Counter cameraCarEnteredCounter() {
        return Counter.builder("metropark.camera.car.entered")
                .description("Number of car entered events from cameras")
                .tag("service", "camera")
                .register(meterRegistry);
    }

    @Bean
    public Counter cameraCarExitedCounter() {
        return Counter.builder("metropark.camera.car.exited")
                .description("Number of car exited events from cameras")
                .tag("service", "camera")
                .register(meterRegistry);
    }

    /**
     * Custom counter for simulation events.
     */
    @Bean
    public Counter simulationEntryCounter() {
        return Counter.builder("metropark.simulation.entry")
                .description("Number of simulation entry events")
                .tag("service", "simulation")
                .register(meterRegistry);
    }

    @Bean
    public Counter simulationExitCounter() {
        return Counter.builder("metropark.simulation.exit")
                .description("Number of simulation exit events")
                .tag("service", "simulation")
                .register(meterRegistry);
    }

    /**
     * Custom gauge for active parking sessions.
     */
    @Bean
    public io.micrometer.core.instrument.Gauge activeSessionsGauge() {
        return io.micrometer.core.instrument.Gauge
                .builder("metropark.parking.active.sessions", this, MetricsConfig::getActiveSessionsCount)
                .description("Number of active parking sessions")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    /**
     * Custom gauge for available parking slots.
     */
    @Bean
    public io.micrometer.core.instrument.Gauge availableSlotsGauge() {
        return io.micrometer.core.instrument.Gauge
                .builder("metropark.parking.available.slots", this, MetricsConfig::getAvailableSlotsCount)
                .description("Number of available parking slots")
                .tag("service", "parking")
                .register(meterRegistry);
    }

    /**
     * Custom gauge for active reservations.
     */
    @Bean
    public io.micrometer.core.instrument.Gauge activeReservationsGauge() {
        return io.micrometer.core.instrument.Gauge
                .builder("metropark.reservation.active", this, MetricsConfig::getActiveReservationsCount)
                .description("Number of active reservations")
                .tag("service", "reservation")
                .register(meterRegistry);
    }

    // These methods would be implemented with actual service calls
    // For now, they return 0 as placeholders
    private double getActiveSessionsCount() {
        // TODO: Implement with actual service call
        return 0;
    }

    private double getAvailableSlotsCount() {
        // TODO: Implement with actual service call
        return 0;
    }

    private double getActiveReservationsCount() {
        // TODO: Implement with actual service call
        return 0;
    }
}
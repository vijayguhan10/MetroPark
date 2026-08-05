package com.example.Metropark.BFF.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.springframework.stereotype.Service;

import com.example.Metropark.BFF.dto.ActiveSessionDto;
import com.example.Metropark.BFF.dto.RevenueDto;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class LiveDataService {

    private static final List<String> VEHICLE_TYPES = Arrays.asList(
        "Toyota Camry", "Honda Civic", "Ford F-150", "BMW X5", 
        "Mercedes C-Class", "Tesla Model 3", "Audi A4", "Nissan Altima"
    );
    
    private static final List<String> PLATES = Arrays.asList(
        "ABC-1234", "XYZ-5678", "DEF-9012", "GHI-3456", "JKL-7890",
        "MNO-2345", "PQR-6789", "STU-0123", "VWX-4567", "YZA-8901"
    );
    
    private static final List<String> STATUSES = Arrays.asList("ACTIVE", "CREATED", "PAYMENT_PENDING");
    
    private static final List<String> SLOTS = Arrays.asList(
        "B1-01", "B1-02", "B1-03", "B1-04", "B1-05",
        "B2-01", "B2-02", "B2-03", "B2-04", "B2-05",
        "B3-01", "B3-02", "B3-03", "B3-04", "B3-05",
        "B4-01", "B4-02", "B4-03", "B4-04", "B4-05",
        "B5-01", "B5-02", "B5-03", "B5-04", "B5-05"
    );

    
    public Flux<ActiveSessionDto> streamLiveSessions() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(tick -> generateRandomSession())
                .onBackpressureDrop();
    }

    
    public Flux<RevenueDto> streamLivePayments() {
        return Flux.interval(Duration.ofSeconds(1))
                .map(tick -> generateRandomRevenue())
                .onBackpressureDrop();
    }

    
    public Flux<ActiveSessionDto> getLiveSessions() {
        int sessionCount = ThreadLocalRandom.current().nextInt(8, 20);
        return Flux.range(1, sessionCount)
                .map(i -> generateRandomSession())
                .delayElements(Duration.ofMillis(100));
    }

    
    public Mono<RevenueDto> getLivePayments() {
        return Mono.just(generateRandomRevenue());
    }

    private List<ActiveSessionDto> generateRandomSessions() {
        int sessionCount = ThreadLocalRandom.current().nextInt(8, 20);
        return ThreadLocalRandom.current().ints(sessionCount, 0, Integer.MAX_VALUE)
                .mapToObj(i -> generateRandomSession())
                .toList();
    }

    private ActiveSessionDto generateRandomSession() {
        String sessionId = "#SN-" + String.format("%06d", ThreadLocalRandom.current().nextInt(100000, 999999));
        String plate = PLATES.get(ThreadLocalRandom.current().nextInt(PLATES.size()));
        String vehicle = VEHICLE_TYPES.get(ThreadLocalRandom.current().nextInt(VEHICLE_TYPES.size()));
        String slot = SLOTS.get(ThreadLocalRandom.current().nextInt(SLOTS.size()));
        String status = STATUSES.get(ThreadLocalRandom.current().nextInt(STATUSES.size()));
        
        LocalDateTime entryTime = LocalDateTime.now().minusMinutes(ThreadLocalRandom.current().nextInt(0, 240));
        int durationMinutes = ThreadLocalRandom.current().nextInt(5, 300);
        
        return new ActiveSessionDto(
            sessionId, plate, vehicle, slot, status, entryTime, durationMinutes
        );
    }

    private RevenueDto generateRandomRevenue() {
        BigDecimal amount = BigDecimal.valueOf(ThreadLocalRandom.current().nextDouble(5000, 50000))
                .setScale(2, java.math.RoundingMode.HALF_UP);
        long transactionCount = ThreadLocalRandom.current().nextLong(50, 500);
        
        return new RevenueDto(amount, "USD", transactionCount, "TODAY");
    }
}

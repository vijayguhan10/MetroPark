package com.example.Metropark.BFF.service;

import com.example.Metropark.BFF.dto.SimulationEventDto;
import com.example.Metropark.BFF.dto.SimulationRunResponseDto;
import com.example.Metropark.BFF.dto.SimulationStateDto;
import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.parking.service.ParkingSessionService;
import com.example.Metropark.parking.service.ParkingSlotService;
import com.example.Metropark.payments.dto.PaymentDto;
import com.example.Metropark.payments.dto.PaymentStatusUpdateDto;
import com.example.Metropark.payments.repo.PaymentMethodRepository;
import com.example.Metropark.payments.service.PaymentService;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.dto.VehicleResponseDto;
import com.example.Metropark.vehicle.service.VehicleService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class SimulationService {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimulationService.class);

    private static final int SIMULATION_SECONDS = 15;
    private static final int SLOTS_PER_LOCATION = 10;
    private static final List<String> VEHICLE_BRANDS = List.of(
            "Maruti Suzuki", "Hyundai", "Tata", "Mahindra", "Toyota", "Honda", "Kia", "MG", "Skoda", "Volkswagen");
    private static final List<String> VEHICLE_MODELS = List.of(
            "Swift", "Baleno", "i20", "Creta", "Nexon", "Harrier", "XUV700", "Scorpio", "Innova", "City", "Seltos",
            "Astor", "Slavia", "Virtus");
    private static final List<String> COLORS = List.of(
            "White", "Black", "Silver", "Gray", "Red", "Blue", "Green", "Brown", "Orange", "Yellow");
    private static final List<String> INDIAN_STATES = List.of(
            "KA", "TN", "MH", "DL", "KL", "AP", "TS", "UP", "WB", "GJ", "RJ", "PB", "HR", "MP", "BR", "OD", "JK",
            "HP", "UK", "CG", "JH", "AS", "ML", "NL", "MN", "MZ", "SK", "TR", "GA", "PY", "LD", "AN", "CH", "DN",
            "DD");

    private final VehicleService vehicleService;
    private final ParkingSlotService parkingSlotService;
    private final ParkingSessionService parkingSessionService;
    private final PaymentService paymentService;
    private final PaymentMethodRepository paymentMethodRepository;
    private final ParkingSlotRepository parkingSlotRepository;
    private final LocationRepository locationRepository;
    private final UserRepository userRepository;

    private final Sinks.Many<SimulationEventDto> eventSink = Sinks.many().multicast().onBackpressureBuffer();
    private final Random random = new Random();
    private final AtomicInteger slotSequence = new AtomicInteger(1000);
    private final AtomicLong plateSequence = new AtomicLong();

    private final ConcurrentHashMap<Integer, VehicleState> activeVehicles = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, String> slotStatus = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Integer> userActiveSession = new ConcurrentHashMap<>();
    private final Set<Integer> simulationSlotIds = ConcurrentHashMap.newKeySet();

    private volatile List<LocationDto> cachedLocations = List.of();
    private volatile List<UserDto> cachedUsers = List.of();

    public SimulationService(
            VehicleService vehicleService,
            ParkingSlotService parkingSlotService,
            ParkingSessionService parkingSessionService,
            PaymentService paymentService,
            PaymentMethodRepository paymentMethodRepository,
            ParkingSlotRepository parkingSlotRepository,
            LocationRepository locationRepository,
            UserRepository userRepository) {
        this.vehicleService = vehicleService;
        this.parkingSlotService = parkingSlotService;
        this.parkingSessionService = parkingSessionService;
        this.paymentService = paymentService;
        this.paymentMethodRepository = paymentMethodRepository;
        this.parkingSlotRepository = parkingSlotRepository;
        this.locationRepository = locationRepository;
        this.userRepository = userRepository;
    }

    public Mono<SimulationRunResponseDto> runSimulation() {
        return Mono.defer(() -> {
            SimulationRunContext context = new SimulationRunContext();
            LOGGER.info("Starting {}-second parking simulation", SIMULATION_SECONDS);
            emitEvent(context, "SIMULATION_STARTED", "Simulation started", null);

            return initializeSimulationData(context)
                    .then(runSimulationCycle(context))
                    .then(finalizeRemainingSessions(context))
                    .then(Mono.fromSupplier(() -> {
                        emitEvent(context, "SIMULATION_ENDED", "Simulation ended", null);
                        LOGGER.info("Simulation completed");
                        return buildRunResponse(context);
                    }));
        });
    }

    public Flux<SimulationEventDto> getEventStream() {
        return eventSink.asFlux();
    }

    private Mono<Void> initializeSimulationData(SimulationRunContext context) {
        resetSimulationState();

        Mono<List<LocationDto>> locationsMono = locationRepository.findAll().collectList();
        Mono<List<UserDto>> usersMono = userRepository.findAll().collectList();
        Mono<List<ParkingSessionDto>> activeSessionsMono = parkingSessionService.getAllSessions()
                .filter(this::isActiveSession)
                .collectList();

        return Mono.zip(locationsMono, usersMono, activeSessionsMono)
                .flatMap(tuple -> {
                    cachedLocations = List.copyOf(tuple.getT1());
                    cachedUsers = List.copyOf(tuple.getT2());
                    seedExistingUserAllocations(tuple.getT3());

                    LOGGER.info("Fetched {} locations and {} users from database", cachedLocations.size(),
                            cachedUsers.size());
                    emitEvent(
                            context,
                            "SIMULATION_DATA_READY",
                            String.format(
                                    "Loaded %d locations, %d users, %d users with active sessions",
                                    cachedLocations.size(),
                                    cachedUsers.size(),
                                    userActiveSession.size()),
                            null);

                    return initializeSlots(context);
                });
    }

    private void resetSimulationState() {
        activeVehicles.clear();
        slotStatus.clear();
        userActiveSession.clear();
        simulationSlotIds.clear();
        cachedLocations = List.of();
        cachedUsers = List.of();
    }

    private void seedExistingUserAllocations(List<ParkingSessionDto> activeSessions) {
        for (ParkingSessionDto session : activeSessions) {
            if (session.userId() != null && session.sessionId() != null) {
                userActiveSession.put(session.userId(), session.sessionId());
            }
        }
    }

    private Mono<Void> initializeSlots(SimulationRunContext context) {
        if (cachedLocations.isEmpty()) {
            emitEvent(context, "NO_LOCATIONS", "No locations found in database", null);
            return Mono.empty();
        }

        List<ParkingSlotDto> slotsToCreate = new ArrayList<>();
        List<String> createdSensorIds = new ArrayList<>();

        for (LocationDto location : cachedLocations) {
            String locationSuffix = location.locationId().substring(Math.max(0, location.locationId().length() - 3))
                    .toUpperCase();

            for (int index = 0; index < SLOTS_PER_LOCATION; index++) {
                int sequence = slotSequence.incrementAndGet();
                String displayCode = String.format("%s-%03d", locationSuffix, sequence);
                String sensorId = String.format("SENSOR-%s-%06d", locationSuffix, sequence);

                slotsToCreate.add(new ParkingSlotDto(
                        null,
                        location.locationId(),
                        displayCode,
                        random.nextInt(4) + 1,
                        1,
                        sensorId,
                        "AVAILABLE"));
                createdSensorIds.add(sensorId);
            }
        }

        return parkingSlotService.createSlots(slotsToCreate)
                .then(parkingSlotRepository.findAll()
                        .filter(slot -> createdSensorIds.contains(slot.sensorId()))
                        .collectList())
                .doOnNext(createdSlots -> {
                    for (ParkingSlotDto slot : createdSlots) {
                        simulationSlotIds.add(slot.slotId());
                        slotStatus.put(slot.slotId(), slot.currentStatus());
                    }
                    LOGGER.info("Created {} parking slots across {} locations", createdSlots.size(),
                            cachedLocations.size());
                })
                .doOnNext(createdSlots -> emitEvent(
                        context,
                        "SLOTS_CREATED",
                        String.format("Created %d parking slots across %d locations", createdSlots.size(),
                                cachedLocations.size()),
                        null))
                .then();
    }

    private Mono<Void> runSimulationCycle(SimulationRunContext context) {
        return Flux.interval(Duration.ofSeconds(1), Schedulers.parallel())
                .take(SIMULATION_SECONDS)
                .concatMap(second -> processSecond(second.intValue() + 1, context))
                .then();
    }

    private Mono<Void> processSecond(int currentSecond, SimulationRunContext context) {
        List<Mono<Void>> actions = new ArrayList<>();

        if (currentSecond % 5 == 0) {
            actions.add(processVehicleExits(currentSecond, context));
        }
        actions.add(processVehicleEntry(currentSecond, context));

        return Mono.whenDelayError(actions)
                .doOnSuccess(ignored -> emitEvent(
                        context,
                        "SECOND_TICK",
                        String.format("Second %d completed", currentSecond),
                        null));
    }

    private Mono<Void> processVehicleEntry(int second, SimulationRunContext context) {
        if (cachedUsers.isEmpty() || cachedLocations.isEmpty() || simulationSlotIds.isEmpty()) {
            LOGGER.debug("Simulation data incomplete at second {}", second);
            return Mono.empty();
        }

        List<UserDto> availableUsers = cachedUsers.stream()
                .filter(this::isEligibleUser)
                .filter(user -> !userActiveSession.containsKey(user.userId()))
                .toList();

        if (availableUsers.isEmpty()) {
            emitEvent(context, "NO_AVAILABLE_USERS", "No eligible users available for a new session", null);
            return Mono.empty();
        }

        UserDto user = availableUsers.get(random.nextInt(availableUsers.size()));
        String userId = user.userId();
        String vehicleNumber = generateIndianNumberPlate();
        Integer vehicleTypeId = random.nextInt(4) + 1;
        LocalDateTime now = LocalDateTime.now();

        VehicleDto vehicleDto = new VehicleDto(
                null,
                userId,
                vehicleNumber,
                vehicleTypeId,
                randomValue(VEHICLE_BRANDS),
                randomValue(VEHICLE_MODELS),
                randomValue(COLORS),
                true,
                now,
                now);

        return findAvailableSlot()
                .switchIfEmpty(Mono.defer(() -> {
                    emitEvent(context, "NO_AVAILABLE_SLOTS", "No available simulation slot found", null);
                    return Mono.empty();
                }))
                .flatMap(slotId -> vehicleService.registerVehicle(vehicleDto)
                        .then(resolveVehicleId(userId, vehicleNumber))
                        .flatMap(vehicleId -> createParkingSession(userId, vehicleNumber, vehicleId, slotId, second,
                                context)))
                .onErrorResume(error -> {
                    LOGGER.error("Error processing vehicle entry at second {}", second, error);
                    emitEvent(context, "VEHICLE_ENTRY_FAILED", error.getMessage(), null);
                    return Mono.empty();
                });
    }

    private Mono<Integer> resolveVehicleId(String userId, String vehicleNumber) {
        return vehicleService.getVehiclesByUserId(userId)
                .filter(vehicle -> vehicleNumber.equalsIgnoreCase(vehicle.vehicleNumber()))
                .next()
                .map(VehicleResponseDto::vehicleId)
                .switchIfEmpty(Mono.error(new IllegalStateException("Unable to resolve registered vehicle ID")));
    }

    private Mono<Void> createParkingSession(
            String userId,
            String vehicleNumber,
            Integer vehicleId,
            Integer slotId,
            int second,
            SimulationRunContext context) {
        ParkingSessionDto sessionDto = new ParkingSessionDto(
                null,
                null,
                slotId,
                userId,
                vehicleId,
                1,
                null,
                "CREATED",
                LocalDateTime.now(),
                null,
                LocalDateTime.now().plusMinutes(random.nextInt(30) + 10),
                0,
                "PENDING",
                1,
                LocalDateTime.now(),
                LocalDateTime.now());

        return parkingSessionService.createSession(sessionDto)
                .flatMap(sessionId -> parkingSlotService.updateSlotStatus(slotId, "OCCUPIED")
                        .then(createPaymentForSession(sessionId, userId, context))
                        .then(Mono.fromRunnable(() -> {
                            activeVehicles.put(sessionId,
                                    new VehicleState(userId, vehicleId, slotId, LocalDateTime.now()));
                            slotStatus.put(slotId, "OCCUPIED");
                            userActiveSession.put(userId, sessionId);
                            context.vehicleEntries.incrementAndGet();
                            emitEvent(
                                    context,
                                    "VEHICLE_ENTRY",
                                    String.format(
                                            "Vehicle entered at second %d: session=%d, slot=%d, user=%s, vehicle=%s",
                                            second,
                                            sessionId,
                                            slotId,
                                            userId,
                                            vehicleNumber),
                                    sessionId);
                        })))
                .then();
    }

    private Mono<Integer> findAvailableSlot() {
        return Flux.fromIterable(simulationSlotIds)
                .filter(slotId -> "AVAILABLE".equalsIgnoreCase(slotStatus.get(slotId)))
                .next();
    }

    private Mono<Void> processVehicleExits(int second, SimulationRunContext context) {
        if (activeVehicles.isEmpty()) {
            emitEvent(context, "NO_ACTIVE_VEHICLES", "No vehicles to exit at second " + second, null);
            return Mono.empty();
        }

        List<Mono<Void>> exitActions = new ArrayList<>();
        for (var entry : new ArrayList<>(activeVehicles.entrySet())) {
            Duration parkedDuration = Duration.between(entry.getValue().entryTime(), LocalDateTime.now());
            if (parkedDuration.getSeconds() >= 5 && random.nextDouble() < 0.5) {
                exitActions.add(processSingleVehicleExit(entry.getKey(), entry.getValue(), context));
            }
        }

        if (exitActions.isEmpty()) {
            emitEvent(context, "NO_EXIT_TRIGGERED", "No vehicle met exit criteria at second " + second, null);
            return Mono.empty();
        }

        return Mono.whenDelayError(exitActions)
                .doOnSuccess(ignored -> emitEvent(
                        context,
                        "VEHICLE_EXITS_PROCESSED",
                        String.format("Processed %d vehicle exits at second %d", exitActions.size(), second),
                        null));
    }

    private Mono<Void> finalizeRemainingSessions(SimulationRunContext context) {
        if (activeVehicles.isEmpty()) {
            return Mono.empty();
        }

        List<Mono<Void>> remainingExits = new ArrayList<>();
        for (var entry : new ArrayList<>(activeVehicles.entrySet())) {
            remainingExits.add(processSingleVehicleExit(entry.getKey(), entry.getValue(), context));
        }

        return Mono.whenDelayError(remainingExits)
                .doOnSuccess(ignored -> emitEvent(
                        context,
                        "FINAL_EXIT_SWEEP",
                        String.format("Completed final exit sweep for %d active sessions", remainingExits.size()),
                        null));
    }

    private Mono<Void> processSingleVehicleExit(int sessionId,
            VehicleState state,
            SimulationRunContext context) {

        return parkingSessionService.getSessionById(sessionId)
                .switchIfEmpty(Mono.<SessionResponse>error(
                        new IllegalStateException("Session not found: " + sessionId)))
                .flatMap(session -> {
                    Integer sessionVersion = session.sessionVersion();
                    int version = sessionVersion != null ? sessionVersion.intValue() : 1;

                    return parkingSessionService.updateSessionStatus(
                            sessionId,
                            "COMPLETED",
                            version)
                        .flatMap(rows -> {
                            if (rows == 0) {
                                return Mono.<Void>error(new IllegalStateException(
                                        "Failed to update session " + sessionId +
                                                ". Possible version mismatch."));
                            }

                            return parkingSlotService
                                    .updateSlotStatus(state.slotId(), "AVAILABLE")
                                    .then(processPaymentCompletion(
                                            sessionId,
                                            state.userId(),
                                            context))
                                    .then(Mono.fromRunnable(() -> {
                                        slotStatus.put(state.slotId(), "AVAILABLE");
                                        activeVehicles.remove(sessionId);
                                        userActiveSession.remove(state.userId(), sessionId);
                                        context.vehicleExits.incrementAndGet();

                                        emitEvent(
                                                context,
                                                "VEHICLE_EXIT",
                                                String.format(
                                                        "Vehicle exited: session=%d, slot=%d released, user=%s",
                                                        sessionId,
                                                        state.slotId(),
                                                        state.userId()),
                                                sessionId);
                                    }));
                        });
                })
                .onErrorResume(error -> {
                    LOGGER.error("Error processing vehicle exit for session {}", sessionId, error);

                    emitEvent(
                            context,
                            "VEHICLE_EXIT_FAILED",
                            error.getMessage(),
                            sessionId);

                    return Mono.<Void>empty();
                });
    }

    private Mono<Void> createPaymentForSession(int sessionId, String userId, SimulationRunContext context) {
        return paymentMethodRepository.findAll()
                .filter(method -> Boolean.TRUE.equals(method.isActive()))
                .next()
                .flatMap(method -> {
                    BigDecimal amount = BigDecimal.valueOf(random.nextDouble(10.0, 50.0))
                            .setScale(2, java.math.RoundingMode.HALF_UP);
                    PaymentDto paymentDto = new PaymentDto(
                            null,
                            "TXN-" + System.currentTimeMillis() + "-" + random.nextInt(10000),
                            sessionId,
                            userId,
                            method.methodId().intValue(),
                            amount,
                            "INR",
                            "PENDING",
                            null,
                            null,
                            null,
                            LocalDateTime.now(),
                            LocalDateTime.now());

                    return paymentService.createPayment(paymentDto)
                            .doOnSuccess(ignored -> {
                                context.paymentsCreated.incrementAndGet();
                                emitEvent(
                                        context,
                                        "PAYMENT_CREATED",
                                        String.format("Payment created for session=%d, user=%s", sessionId, userId),
                                        sessionId);
                            })
                            .then();
                })
                .switchIfEmpty(Mono.<Void>fromRunnable(() -> emitEvent(
                        context,
                        "PAYMENT_METHOD_MISSING",
                        "No active payment method found for simulation billing",
                        sessionId)))
                .onErrorResume(error -> {
                    LOGGER.warn("Payment creation failed for session {}", sessionId, error);
                    emitEvent(context, "PAYMENT_CREATION_FAILED", error.getMessage(), sessionId);
                    return Mono.empty();
                });
    }

    private Mono<Void> processPaymentCompletion(int sessionId, String userId, SimulationRunContext context) {
        return paymentService.getPaymentsBySessionId(sessionId)
                .next()
                .flatMap(payment -> paymentService.updatePaymentStatus(
                        payment.paymentId(),
                        new PaymentStatusUpdateDto("SUCCESS", "SYSTEM", "Simulation payment",
                                "SIM-" + System.currentTimeMillis()))
                        .doOnSuccess(ignored -> {
                            context.paymentsCompleted.incrementAndGet();
                            emitEvent(
                                    context,
                                    "PAYMENT_COMPLETED",
                                    String.format("Payment completed for session=%d, user=%s", sessionId, userId),
                                    sessionId);
                        })
                        .then())
                .switchIfEmpty(Mono.<Void>fromRunnable(() -> emitEvent(
                        context,
                        "PAYMENT_NOT_FOUND",
                        "No payment record found for session " + sessionId,
                        sessionId)))
                .onErrorResume(error -> {
                    LOGGER.warn("Payment completion failed for session {}", sessionId, error);
                    emitEvent(context, "PAYMENT_COMPLETION_FAILED", error.getMessage(), sessionId);
                    return Mono.empty();
                });
    }

    private String generateIndianNumberPlate() {
        long sequence = plateSequence.getAndIncrement();
        int serial = (int) (sequence % 10_000);
        sequence /= 10_000;
        int secondLetterIndex = (int) (sequence % 26);
        sequence /= 26;
        int firstLetterIndex = (int) (sequence % 26);
        sequence /= 26;
        int rto = (int) (sequence % 99) + 1;
        sequence /= 99;
        String state = INDIAN_STATES.get((int) (sequence % INDIAN_STATES.size()));

        return String.format(
                "%s%02d%c%c%04d",
                state,
                rto,
                (char) ('A' + firstLetterIndex),
                (char) ('A' + secondLetterIndex),
                serial);
    }

    private SimulationRunResponseDto buildRunResponse(SimulationRunContext context) {
        List<SimulationStateDto> states = List.of(
                new SimulationStateDto("Vehicle Exit", "Monitoring",
                        context.vehicleExits.get() > 0 ? "COMPLETED" : "SKIPPED"),
                new SimulationStateDto("Exit Event", "Monitoring",
                        context.vehicleExits.get() > 0 ? "EMITTED" : "SKIPPED"),
                new SimulationStateDto("Message Queue", "Monitoring",
                        context.queueMessages.get() > 0 ? "PUBLISHED" : "IDLE"),
                new SimulationStateDto("Billing Worker", "Monitoring",
                        context.paymentsCompleted.get() > 0 ? "COMPLETED" : "PENDING"),
                new SimulationStateDto("Wallet Service", "Billing", null),
                new SimulationStateDto("Payment Service", "Monitoring",
                        context.paymentsCreated.get() > 0 || context.paymentsCompleted.get() > 0 ? "COMPLETED"
                                : "IDLE"),
                new SimulationStateDto("Result", "Monitoring", "SUCCESS"));

        return new SimulationRunResponseDto("COMPLETED", states, List.copyOf(context.events));
    }

    private void emitEvent(SimulationRunContext context, String type, String message, Integer sessionId) {
        SimulationEventDto event = new SimulationEventDto(type, message, sessionId, LocalDateTime.now());
        context.events.add(event);
        context.queueMessages.incrementAndGet();
        eventSink.tryEmitNext(event);
    }

    private boolean isEligibleUser(UserDto user) {
        return user != null && user.userId() != null && "ACTIVE".equalsIgnoreCase(user.userStatus());
    }

    private boolean isActiveSession(ParkingSessionDto session) {
        return session != null
                && session.sessionId() != null
                && session.userId() != null
                && session.sessionStatus() != null
                && ("CREATED".equalsIgnoreCase(session.sessionStatus())
                        || "ACTIVE".equalsIgnoreCase(session.sessionStatus()));
    }

    private String randomValue(List<String> values) {
        return values.get(random.nextInt(values.size()));
    }

    private record VehicleState(String userId, Integer vehicleId, Integer slotId, LocalDateTime entryTime) {
    }

    private static final class SimulationRunContext {
        private final List<SimulationEventDto> events = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger vehicleEntries = new AtomicInteger();
        private final AtomicInteger vehicleExits = new AtomicInteger();
        private final AtomicInteger paymentsCreated = new AtomicInteger();
        private final AtomicInteger paymentsCompleted = new AtomicInteger();
        private final AtomicInteger queueMessages = new AtomicInteger();
    }
}

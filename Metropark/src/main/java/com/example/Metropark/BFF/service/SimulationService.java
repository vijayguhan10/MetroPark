package com.example.Metropark.BFF.service;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.BFF.dto.SimulationEventDto;
import com.example.Metropark.BFF.dto.SimulationRunResponseDto;
import com.example.Metropark.BFF.dto.SimulationStateDto;
import com.example.Metropark.camera.service.CameraService;
import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.gate.repo.GateRepository;
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
import com.example.Metropark.payments.service.PaymentMethodService;
import com.example.Metropark.payments.service.PaymentService;
import com.example.Metropark.redis.RedisStateService;
import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.service.ReservationClassService;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.dto.VehicleResponseDto;
import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.service.VehicleService;
import com.example.Metropark.vehicle.service.VehicleTypeService;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;

@Service
public class SimulationService {

        private static final Logger LOGGER = LoggerFactory.getLogger(SimulationService.class);

        private final VehicleService vehicleService;
        private final ParkingSlotService parkingSlotService;
        private final ParkingSessionService parkingSessionService;
        private final PaymentService paymentService;
        private final PaymentMethodService paymentMethodService;
        private final PaymentMethodRepository paymentMethodRepository;
        private final ParkingSlotRepository parkingSlotRepository;
        private final GateRepository gateRepository;
        private final LocationRepository locationRepository;
        private final UserRepository userRepository;
        private final VehicleTypeService vehicleTypeService;
        private final ReservationClassService reservationClassService;
        private final CameraService cameraService;
        private final RedisStateService redisStateService;

        private final Sinks.Many<SimulationEventDto> eventSink = Sinks.many().multicast().onBackpressureBuffer();
        private final Random random = new Random();
        private final AtomicBoolean simulationRunning = new AtomicBoolean(false);

        private final ConcurrentHashMap<Integer, VehicleState> activeVehicles = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, String> slotStatus = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Integer> userActiveSession = new ConcurrentHashMap<>();
        private final Set<Integer> simulationSlotIds = ConcurrentHashMap.newKeySet();

        private volatile List<LocationDto> cachedLocations = List.of();
        private volatile List<UserDto> cachedUsers = List.of();
        private volatile List<VehicleResponseDto> cachedVehicles = List.of();
        private volatile List<Integer> cachedGateIds = List.of();
        private volatile List<ParkingSlotDto> cachedSlots = List.of();
        private volatile SimulationRunContext currentSimulationContext;

        // For graceful shutdown
        private volatile Disposable simulationDisposable;
        private volatile Disposable entryDisposable;
        private volatile Disposable exitDisposable;

        public SimulationService(
                        VehicleService vehicleService,
                        ParkingSlotService parkingSlotService,
                        ParkingSessionService parkingSessionService,
                        PaymentService paymentService,
                        PaymentMethodService paymentMethodService,
                        PaymentMethodRepository paymentMethodRepository,
                        ParkingSlotRepository parkingSlotRepository,
                        GateRepository gateRepository,
                        LocationRepository locationRepository,
                        UserRepository userRepository,
                        VehicleTypeService vehicleTypeService,
                        ReservationClassService reservationClassService,
                        CameraService cameraService,
                        RedisStateService redisStateService) {
                this.vehicleService = vehicleService;
                this.parkingSlotService = parkingSlotService;
                this.parkingSessionService = parkingSessionService;
                this.paymentService = paymentService;
                this.paymentMethodService = paymentMethodService;
                this.paymentMethodRepository = paymentMethodRepository;
                this.parkingSlotRepository = parkingSlotRepository;
                this.gateRepository = gateRepository;
                this.locationRepository = locationRepository;
                this.userRepository = userRepository;
                this.vehicleTypeService = vehicleTypeService;
                this.reservationClassService = reservationClassService;
                this.cameraService = cameraService;
                this.redisStateService = redisStateService;
        }

        public Mono<SimulationRunResponseDto> runSimulation() {
                return Mono.defer(() -> {
                        if (!simulationRunning.compareAndSet(false, true)) {
                                SimulationRunContext runningContext = currentSimulationContext;
                                return Mono.just(buildRunResponse("RUNNING",
                                                runningContext != null ? runningContext : new SimulationRunContext()));
                        }

                        SimulationRunContext context = new SimulationRunContext();
                        currentSimulationContext = context;
                        LOGGER.info("Starting continuous parking simulation");
                        emitEvent(context, "SIMULATION_STARTED", "Simulation started", null);

                        simulationDisposable = startSimulationInBackground(context);
                        return Mono.just(buildRunResponse("STARTED", context));
                });
        }

        public Mono<Void> stopSimulation() {
                return Mono.defer(() -> {
                        if (simulationRunning.compareAndSet(true, false)) {
                                if (simulationDisposable != null && !simulationDisposable.isDisposed()) {
                                        simulationDisposable.dispose();
                                        LOGGER.info("Simulation stopped gracefully");
                                        if (currentSimulationContext != null) {
                                                emitEvent(currentSimulationContext, "SIMULATION_STOPPED",
                                                                "Simulation stopped by user", null);
                                        }
                                }
                                return Mono.empty();
                        }
                        return Mono.justOrEmpty(currentSimulationContext)
                                        .flatMap(ctx -> {
                                                emitEvent(ctx, "SIMULATION_ALREADY_STOPPED",
                                                                "Simulation was not running", null);
                                                return Mono.empty();
                                        });
                });
        }

        public Flux<SimulationEventDto> getEventStream() {
                return eventSink.asFlux();
        }

        private Mono<Void> initializeSimulationData(SimulationRunContext context) {
                resetSimulationState();

                Mono<List<LocationDto>> locationsMono = locationRepository.findAll().collectList();
                Mono<List<UserDto>> usersMono = userRepository.findAll().collectList();
                Mono<List<GateDto>> gatesMono = gateRepository.findAll().collectList();
                Mono<List<ParkingSessionDto>> activeSessionsMono = parkingSessionService.getAllSessionsFromDb()
                                .filter(this::isActiveSession)
                                .collectList();

                return Mono.zip(locationsMono, usersMono, gatesMono, activeSessionsMono)
                                .flatMap(tuple -> {
                                        cachedLocations = List.copyOf(tuple.getT1());
                                        cachedUsers = List.copyOf(tuple.getT2());
                                        cachedGateIds = tuple.getT3().stream()
                                                        .map(gate -> gate.gateId())
                                                        .filter(java.util.Objects::nonNull)
                                                        .toList();
                                        seedExistingUserAllocations(tuple.getT4());

                                        LOGGER.info("Fetched {} locations and {} users from database",
                                                        cachedLocations.size(),
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
                generatedVehicleNumbers.clear();
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

                // Fetch valid vehicle types and reservation classes
                return vehicleTypeService.getAllVehicleTypes()
                                .collectList()
                                .zipWith(reservationClassService.getAllReservationClasses().collectList())
                                .flatMap(tuple -> {
                                        List<VehicleTypeDto> vehicleTypes = tuple.getT1();
                                        List<ReservationClassDto> reservationClasses = tuple.getT2();

                                        if (vehicleTypes.isEmpty()) {
                                                emitEvent(context, "NO_VEHICLE_TYPES",
                                                                "No vehicle types found in database", null);
                                                return Mono.empty();
                                        }
                                        if (reservationClasses.isEmpty()) {
                                                emitEvent(context, "NO_RESERVATION_CLASSES",
                                                                "No reservation classes found in database", null);
                                                return Mono.empty();
                                        }

                                        List<Integer> vehicleTypeIds = vehicleTypes.stream()
                                                        .map(vehicleType -> vehicleType.vehicleTypeId())
                                                        .toList();
                                        List<Integer> reservationClassIds = reservationClasses.stream()
                                                        .map(reservationClass -> reservationClass.classId())
                                                        .toList();
                                        cachedVehicleTypeIds = List.copyOf(vehicleTypeIds);

                                        // First, find the highest existing sequence number for each location
                                        return parkingSlotRepository.findAll()
                                                        .collectList()
                                                        .flatMap(existingSlots -> {
                                                                // Build a map of locationId -> max sequence number
                                                                Map<String, Integer> maxSequences = new ConcurrentHashMap<>();
                                                                for (ParkingSlotDto slot : existingSlots) {
                                                                        String displayCode = slot.displayCode();
                                                                        if (displayCode != null
                                                                                        && displayCode.contains("-")) {
                                                                                String[] parts = displayCode.split("-");
                                                                                if (parts.length == 2) {
                                                                                        try {
                                                                                                int seq = Integer
                                                                                                                .parseInt(parts[1]);
                                                                                                maxSequences.merge(slot
                                                                                                                .locationId(),
                                                                                                                seq,
                                                                                                                (currentMax, incoming) -> Math
                                                                                                                                .max(currentMax,
                                                                                                                                                incoming));
                                                                                        } catch (NumberFormatException ignored) {
                                                                                        }
                                                                                }
                                                                        }
                                                                }

                                                                List<ParkingSlotDto> slotsToCreate = new ArrayList<>();
                                                                List<String> createdSensorIds = new ArrayList<>();

                                                                for (LocationDto location : cachedLocations) {
                                                                        String locationSuffix = location.locationId()
                                                                                        .substring(Math.max(0, location
                                                                                                        .locationId()
                                                                                                        .length() - 3))
                                                                                        .toUpperCase();

                                                                        // Get the max existing sequence for this
                                                                        // location, or start from 0
                                                                        int startSequence = maxSequences.getOrDefault(
                                                                                        location.locationId(), 0);
                                                                        AtomicInteger locationSequence = new AtomicInteger(
                                                                                        startSequence);

                                                                        for (int index = 0; index < SLOTS_PER_LOCATION; index++) {
                                                                                int sequence = locationSequence
                                                                                                .incrementAndGet();
                                                                                String displayCode = String.format(
                                                                                                "%s-%03d",
                                                                                                locationSuffix,
                                                                                                sequence);
                                                                                String sensorId = String.format(
                                                                                                "SENSOR-%s-%06d",
                                                                                                locationSuffix,
                                                                                                sequence);

                                                                                // Use valid vehicle type ID and
                                                                                // reservation class ID
                                                                                Integer vehicleTypeId = vehicleTypeIds
                                                                                                .get(random.nextInt(
                                                                                                                vehicleTypeIds.size()));
                                                                                Integer reservationClassId = reservationClassIds
                                                                                                .get(random.nextInt(
                                                                                                                reservationClassIds
                                                                                                                                .size()));

                                                                                slotsToCreate.add(new ParkingSlotDto(
                                                                                                null,
                                                                                                location.locationId(),
                                                                                                displayCode,
                                                                                                vehicleTypeId,
                                                                                                reservationClassId,
                                                                                                sensorId,
                                                                                                "AVAILABLE"));
                                                                                createdSensorIds.add(sensorId);
                                                                        }
                                                                }

                                                                return parkingSlotService.createSlots(slotsToCreate)
                                                                                .then(parkingSlotRepository.findAll()
                                                                                                .filter(slot -> createdSensorIds
                                                                                                                .contains(slot.sensorId()))
                                                                                                .collectList())
                                                                                .doOnNext(createdSlots -> {
                                                                                        for (ParkingSlotDto slot : createdSlots) {
                                                                                                simulationSlotIds.add(
                                                                                                                slot.slotId());
                                                                                                slotStatus.put(slot
                                                                                                                .slotId(),
                                                                                                                slot.currentStatus());
                                                                                        }
                                                                                        LOGGER.info("Created {} parking slots across {} locations",
                                                                                                        createdSlots.size(),
                                                                                                        cachedLocations.size());
                                                                                })
                                                                                .doOnNext(createdSlots -> emitEvent(
                                                                                                context,
                                                                                                "SLOTS_CREATED",
                                                                                                String.format("Created %d parking slots across %d locations",
                                                                                                                createdSlots.size(),
                                                                                                                cachedLocations.size()),
                                                                                                null))
                                                                                .then();
                                                        });
                                });
        }

        private Disposable startSimulationInBackground(SimulationRunContext context) {
                return initializeSimulationData(context)
                                .doOnSuccess(ignored -> emitEvent(context, "SIMULATION_INITIALIZED",
                                                "Simulation initialized; entering continuous run", null))
                                .thenMany(Flux.interval(Duration.ZERO, Duration.ofSeconds(1), Schedulers.parallel())
                                                .onBackpressureDrop() // Drop ticks if downstream can't keep up
                                                .concatMap(tick -> processSecond(Math.toIntExact(tick) + 1, context)
                                                                .onErrorResume(error -> {
                                                                        LOGGER.error(
                                                                                        "Error processing simulation tick {}",
                                                                                        tick + 1,
                                                                                        error);
                                                                        emitEvent(context, "SIMULATION_TICK_FAILED",
                                                                                        error.getMessage(), null);
                                                                        return Mono.<Void>empty();
                                                                })))
                                .doOnError(error -> {
                                        LOGGER.error("Continuous simulation failed", error);
                                        emitEvent(context, "SIMULATION_FAILED", error.getMessage(), null);
                                })
                                .doFinally(signalType -> {
                                        simulationRunning.set(false);
                                        LOGGER.info("Simulation stream completed with signal: {}", signalType);
                                })
                                .subscribe();
        }

        private Mono<Void> processSecond(int currentSecond, SimulationRunContext context) {
                List<Mono<Void>> actions = new ArrayList<>();

                // Process exits every 2 seconds instead of 5
                if (currentSecond % 2 == 0) {
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

                return findAvailableUser()
                                .flatMap(user -> {
                                        String userId = user.userId();
                                        if (cachedVehicleTypeIds.isEmpty()) {
                                                emitEvent(context, "NO_VEHICLE_TYPES",
                                                                "No vehicle types available for simulation entry",
                                                                null);
                                                return Mono.empty();
                                        }

                                        if (cachedGateIds.isEmpty()) {
                                                emitEvent(context, "NO_GATES",
                                                                "No gates found in database for simulation entry",
                                                                null);
                                                return Mono.empty();
                                        }

                                        Integer vehicleTypeId = randomValue(cachedVehicleTypeIds);
                                        Integer gateId = randomValue(cachedGateIds);
                                        LocalDateTime now = LocalDateTime.now();

                                        return findAvailableSlot()
                                                        .switchIfEmpty(Mono.defer(() -> {
                                                                emitEvent(context, "NO_AVAILABLE_SLOTS",
                                                                                "No available simulation slot found",
                                                                                null);
                                                                return Mono.<Integer>empty();
                                                        }))
                                                        .flatMap(slotId -> {
                                                                // Check if user already owns a vehicle that is NOT
                                                                // currently parked
                                                                return vehicleService.getVehiclesByUserId(userId)
                                                                                .next()
                                                                                .onErrorResume(error -> {
                                                                                        // Handle "No vehicles found"
                                                                                        // error as empty
                                                                                        if (error instanceof IllegalArgumentException
                                                                                                        && error.getMessage()
                                                                                                                        .contains("No vehicles found")) {
                                                                                                return Mono.<VehicleResponseDto>empty();
                                                                                        }
                                                                                        return Mono.error(error);
                                                                                })
                                                                                .flatMap(existingVehicle -> {
                                                                                        // Check the database directly
                                                                                        // so we
                                                                                        // do not rely on stale Redis
                                                                                        // state
                                                                                        return parkingSessionService
                                                                                                        .hasActiveSession(
                                                                                                                        existingVehicle.vehicleId())
                                                                                                        .flatMap(hasActiveSession -> {
                                                                                                                if (hasActiveSession) {
                                                                                                                        // Vehicle
                                                                                                                        // is
                                                                                                                        // already
                                                                                                                        // parked,
                                                                                                                        // generate
                                                                                                                        // new
                                                                                                                        // vehicle
                                                                                                                        // instead
                                                                                                                        emitEvent(context,
                                                                                                                                        "VEHICLE_ALREADY_PARKED",
                                                                                                                                        String.format("Vehicle %s for user %s is already parked, generating new vehicle",
                                                                                                                                                        existingVehicle.vehicleNumber(),
                                                                                                                                                        userId),
                                                                                                                                        null);
                                                                                                                        return generateUniqueVehicleNumber()
                                                                                                                                        .flatMap(vehicleNumber -> createNewVehicleAndSession(
                                                                                                                                                        userId,
                                                                                                                                                        vehicleNumber,
                                                                                                                                                        vehicleTypeId,
                                                                                                                                                        slotId,
                                                                                                                                                        gateId,
                                                                                                                                                        second,
                                                                                                                                                        now,
                                                                                                                                                        context));
                                                                                                                } else {
                                                                                                                        // Vehicle
                                                                                                                        // is
                                                                                                                        // free,
                                                                                                                        // reuse
                                                                                                                        // it
                                                                                                                        String vehicleNumber = existingVehicle
                                                                                                                                        .vehicleNumber();
                                                                                                                        Integer vehicleId = existingVehicle
                                                                                                                                        .vehicleId();
                                                                                                                        emitEvent(context,
                                                                                                                                        "VEHICLE_REUSED",
                                                                                                                                        String.format("Reusing existing vehicle %s for user %s",
                                                                                                                                                        vehicleNumber,
                                                                                                                                                        userId),
                                                                                                                                        null);
                                                                                                                        return createParkingSession(
                                                                                                                                        userId,
                                                                                                                                        vehicleNumber,
                                                                                                                                        vehicleId,
                                                                                                                                        slotId,
                                                                                                                                        gateId,
                                                                                                                                        second,
                                                                                                                                        context)
                                                                                                                                        .onErrorResume(error -> handleDuplicateVehicleSession(
                                                                                                                                                        error,
                                                                                                                                                        userId,
                                                                                                                                                        vehicleTypeId,
                                                                                                                                                        slotId,
                                                                                                                                                        gateId,
                                                                                                                                                        second,
                                                                                                                                                        now,
                                                                                                                                                        context));
                                                                                                                }
                                                                                                        });
                                                                                })
                                                                                .switchIfEmpty(
                                                                                                // User doesn't own a
                                                                                                // vehicle - generate
                                                                                                // new one
                                                                                                generateUniqueVehicleNumber()
                                                                                                                .flatMap(vehicleNumber -> createNewVehicleAndSession(
                                                                                                                                userId,
                                                                                                                                vehicleNumber,
                                                                                                                                vehicleTypeId,
                                                                                                                                slotId,
                                                                                                                                gateId,
                                                                                                                                second,
                                                                                                                                now,
                                                                                                                                context)));
                                                        })
                                                        .onErrorResume(error -> {
                                                                LOGGER.error("Error processing vehicle entry at second {}",
                                                                                second, error);
                                                                emitEvent(context, "VEHICLE_ENTRY_FAILED",
                                                                                error.getMessage(), null);
                                                                return Mono.<Void>empty();
                                                        });
                                })
                                .switchIfEmpty(Mono.defer(() -> {
                                        emitEvent(context, "WAITING_FOR_AVAILABLE_USER",
                                                        "No eligible users available right now; waiting for the next exit",
                                                        null);
                                        return Mono.<Void>empty();
                                }));
        }

        private Mono<Void> createNewVehicleAndSession(String userId, String vehicleNumber,
                        Integer vehicleTypeId, Integer slotId, Integer gateId, int second,
                        LocalDateTime now, SimulationRunContext context) {
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

                return vehicleService.registerVehicle(vehicleDto)
                                .then(resolveVehicleId(userId, vehicleNumber))
                                .flatMap(vehicleId -> createParkingSession(
                                                userId, vehicleNumber, vehicleId, slotId, gateId, second, context));
        }

        private Mono<Void> handleDuplicateVehicleSession(Throwable error,
                        String userId,
                        Integer vehicleTypeId,
                        Integer slotId,
                        Integer gateId,
                        int second,
                        LocalDateTime now,
                        SimulationRunContext context) {
                if (!isDuplicateVehicleSessionError(error)) {
                        return Mono.error(error);
                }

                LOGGER.warn("Vehicle session rejected for user {} due to an existing parked or pending vehicle; retrying with a new vehicle",
                                userId);
                emitEvent(context, "VEHICLE_SESSION_RETRY",
                                "Existing vehicle was rejected as already parked or pending; generating a new vehicle",
                                null);

                return generateUniqueVehicleNumber()
                                .flatMap(vehicleNumber -> createNewVehicleAndSession(
                                                userId,
                                                vehicleNumber,
                                                vehicleTypeId,
                                                slotId,
                                                gateId,
                                                second,
                                                now,
                                                context));
        }

        private Mono<UserDto> findAvailableUser() {
                return findAvailableUserWithRetry(0);
        }

        private Mono<UserDto> findAvailableUserWithRetry(int retryCount) {
                if (retryCount >= 10) {
                        return Mono.empty();
                }
                return Flux.fromIterable(cachedUsers)
                                .filter(this::isEligibleUser)
                                .filter(user -> !userActiveSession.containsKey(user.userId()))
                                .next()
                                .switchIfEmpty(
                                                Mono.delay(Duration.ofMillis(500), Schedulers.parallel())
                                                                .then(Mono.defer(() -> findAvailableUserWithRetry(
                                                                                retryCount + 1))));
        }

        private Mono<Integer> resolveVehicleId(String userId, String vehicleNumber) {
                return vehicleService.getVehiclesByUserId(userId)
                                .filter(vehicle -> vehicleNumber.equalsIgnoreCase(vehicle.vehicleNumber()))
                                .next()
                                .map(vehicle -> vehicle.vehicleId())
                                .switchIfEmpty(Mono.error(
                                                new IllegalStateException("Unable to resolve registered vehicle ID")));
        }

        private Mono<Void> createParkingSession(
                        String userId,
                        String vehicleNumber,
                        Integer vehicleId,
                        Integer slotId,
                        Integer gateId,
                        int second,
                        SimulationRunContext context) {
                ParkingSessionDto sessionDto = new ParkingSessionDto(
                                null,
                                null,
                                slotId,
                                userId,
                                vehicleId,
                                gateId,
                                gateId,
                                "CREATED", // Use CREATED to match DB constraint
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
                                                                        new VehicleState(userId, vehicleId, slotId,
                                                                                        LocalDateTime.now()));
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
                return parkingSlotRepository.findAll()
                                .filter(slot -> simulationSlotIds.contains(slot.slotId()))
                                .filter(slot -> "AVAILABLE".equalsIgnoreCase(slot.currentStatus()))
                                .next()
                                .map(slot -> slot.slotId());
        }

        private Mono<Void> processVehicleExits(int second, SimulationRunContext context) {
                if (activeVehicles.isEmpty()) {
                        emitEvent(context, "NO_ACTIVE_VEHICLES", "No vehicles to exit at second " + second, null);
                        return Mono.empty();
                }

                // Process only ONE vehicle exit at a time
                List<Mono<Void>> exitActions = new ArrayList<>();
                for (var entry : new ArrayList<>(activeVehicles.entrySet())) {
                        Duration parkedDuration = Duration.between(entry.getValue().entryTime(), LocalDateTime.now());
                        // Exit if parked for at least 10 seconds with 30% chance, or 30 seconds with
                        // 80% chance
                        boolean shouldExit = (parkedDuration.getSeconds() >= 10 && random.nextDouble() < 0.3) ||
                                        (parkedDuration.getSeconds() >= 30 && random.nextDouble() < 0.8) ||
                                        (parkedDuration.getSeconds() >= 60); // Force exit after 60 seconds
                        if (shouldExit) {
                                exitActions.add(processSingleVehicleExit(entry.getKey(), entry.getValue(), context));
                                break; // Only process one exit at a time
                        }
                }

                if (exitActions.isEmpty()) {
                        emitEvent(context, "NO_EXIT_TRIGGERED", "No vehicle met exit criteria at second " + second,
                                        null);
                        return Mono.empty();
                }

                return Mono.whenDelayError(exitActions)
                                .doOnSuccess(ignored -> emitEvent(
                                                context,
                                                "VEHICLE_EXITS_PROCESSED",
                                                String.format("Processed %d vehicle exits at second %d",
                                                                exitActions.size(), second),
                                                null));
        }

        private Mono<Void> processSingleVehicleExit(int sessionId,
                        VehicleState state,
                        SimulationRunContext context) {

                return parkingSessionService.getSessionById(sessionId)
                                .switchIfEmpty(Mono.error(
                                                new IllegalStateException("Session not found: " + sessionId)))
                                .flatMap(session -> {
                                        Integer sessionVersion = session.sessionVersion();
                                        int version = sessionVersion != null ? sessionVersion : 1;

                                        // First update session to ACTIVE (vehicle is exiting)
                                        return parkingSessionService.updateSessionStatus(
                                                        sessionId,
                                                        "ACTIVE",
                                                        version)
                                                        .flatMap(rows -> {
                                                                if (rows == 0) {
                                                                        return Mono.<Void>error(
                                                                                        new IllegalStateException(
                                                                                                        "Failed to update session "
                                                                                                                        + sessionId
                                                                                                                        +
                                                                                                                        " to ACTIVE. Possible version mismatch."));
                                                                }

                                                                // Then complete the session
                                                                return parkingSessionService.updateSessionStatus(
                                                                                sessionId,
                                                                                "COMPLETED",
                                                                                version + 1)
                                                                                .flatMap(completedRows -> {
                                                                                        if (completedRows == 0) {
                                                                                                return Mono.<Void>error(
                                                                                                                new IllegalStateException(
                                                                                                                                "Failed to update session "
                                                                                                                                                + sessionId
                                                                                                                                                +
                                                                                                                                                " to COMPLETED. Possible version mismatch."));
                                                                                        }

                                                                                        return parkingSlotService
                                                                                                        .updateSlotStatus(
                                                                                                                        state.slotId(),
                                                                                                                        "AVAILABLE")
                                                                                                        .then(processPaymentCompletion(
                                                                                                                        sessionId,
                                                                                                                        state.userId(),
                                                                                                                        context))
                                                                                                        .then(Mono.<Void>fromRunnable(
                                                                                                                        () -> {
                                                                                                                                slotStatus.put(state
                                                                                                                                                .slotId(),
                                                                                                                                                "AVAILABLE");
                                                                                                                                activeVehicles.remove(
                                                                                                                                                sessionId);
                                                                                                                                userActiveSession
                                                                                                                                                .remove(
                                                                                                                                                                state.userId(),
                                                                                                                                                                sessionId);
                                                                                                                                context.vehicleExits
                                                                                                                                                .incrementAndGet();

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
                                                        "TXN-" + System.currentTimeMillis() + "-"
                                                                        + random.nextInt(10000),
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
                                                                                String.format("Payment created for session=%d, user=%s",
                                                                                                sessionId, userId),
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
                                                new PaymentStatusUpdateDto("PAID", "SYSTEM",
                                                                "Simulation payment completed on exit",
                                                                "SIM-" + System.currentTimeMillis()))
                                                .doOnSuccess(ignored -> {
                                                        context.paymentsCompleted.incrementAndGet();
                                                        emitEvent(
                                                                        context,
                                                                        "PAYMENT_COMPLETED",
                                                                        String.format("Payment completed for session=%d, user=%s on exit",
                                                                                        sessionId, userId),
                                                                        sessionId);
                                                })
                                                .then())
                                .switchIfEmpty(Mono.defer(() -> {
                                        // If no payment exists, create one and complete it
                                        LOGGER.warn("No payment found for session {}, creating and completing payment",
                                                        sessionId);
                                        return createPaymentForSession(sessionId, userId, context)
                                                        .then(processPaymentCompletion(sessionId, userId, context));
                                }))
                                .onErrorResume(error -> {
                                        LOGGER.warn("Payment completion failed for session {}", sessionId, error);
                                        emitEvent(context, "PAYMENT_COMPLETION_FAILED", error.getMessage(), sessionId);
                                        return Mono.empty();
                                });
        }

        private static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        private String generateIndianNumberPlate() {
                while (true) {
                        String state = randomValue(INDIAN_STATES);
                        int rto = random.nextInt(99) + 1;
                        char first = LETTERS.charAt(random.nextInt(26));
                        char second = LETTERS.charAt(random.nextInt(26));
                        int serial = random.nextInt(10000);

                        String plate = String.format(
                                        "%s%02d%c%c%04d",
                                        state,
                                        rto,
                                        first,
                                        second,
                                        serial);

                        if (!generatedVehicleNumbers.contains(plate)) {
                                generatedVehicleNumbers.add(plate);
                                return plate;
                        }
                }
        }

        private Mono<String> generateUniqueVehicleNumber() {
                return Mono.defer(() -> {
                        String plate = generateIndianNumberPlate();
                        return vehicleService
                                        .getVehicleByNumber(plate)
                                        .hasElement()
                                        .flatMap(exists -> {
                                                if (exists) {
                                                        return generateUniqueVehicleNumber();
                                                }
                                                return Mono.just(plate);
                                        });
                });
        }

        private SimulationRunResponseDto buildRunResponse(String status, SimulationRunContext context) {
                List<SimulationStateDto> states = List.of(
                                new SimulationStateDto("Vehicle Exit", "Monitoring", "RUNNING"),
                                new SimulationStateDto("Exit Event", "Monitoring", "RUNNING"),
                                new SimulationStateDto("Message Queue", "Monitoring", "RUNNING"),
                                new SimulationStateDto("Billing Worker", "Monitoring", "RUNNING"),
                                new SimulationStateDto("Wallet Service", "Billing", "RUNNING"),
                                new SimulationStateDto("Payment Service", "Monitoring", "RUNNING"),
                                new SimulationStateDto("Result", "Monitoring", status));

                return new SimulationRunResponseDto(status, states, List.copyOf(context.events));
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
                                && ("RESERVED".equalsIgnoreCase(session.sessionStatus())
                                                || "CREATED".equalsIgnoreCase(session.sessionStatus())
                                                || "ACTIVE".equalsIgnoreCase(session.sessionStatus()));
        }

        private boolean isDuplicateVehicleSessionError(Throwable error) {
                if (error == null) {
                        return false;
                }

                String message = error.getMessage();
                return error instanceof IllegalStateException
                                && message != null
                                && message.contains("already parked or has a pending entry in another location");
        }

        private <T> T randomValue(List<T> values) {
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

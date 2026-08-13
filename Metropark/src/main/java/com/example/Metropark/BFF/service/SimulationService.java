package com.example.Metropark.BFF.service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.event.CameraEventPublisher;
import com.example.Metropark.camera.event.CameraEventType;
import com.example.Metropark.gate.repo.GateRepository;
import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.parking.service.ParkingLifecycleService;
import com.example.Metropark.parking.service.ParkingSessionService;
import com.example.Metropark.parking.service.ParkingSlotService;
import com.example.Metropark.payments.payment.repo.PaymentMethodRepository;
import com.example.Metropark.reservation.service.ReservationClassService;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.dto.VehicleDto;
import com.example.Metropark.vehicle.service.VehicleService;
import com.example.Metropark.vehicle.service.VehicleTypeService;

import jakarta.annotation.PreDestroy;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.core.scheduler.Schedulers;
import reactor.util.retry.Retry;

@Service
public class SimulationService {

        private static final Logger LOGGER = LoggerFactory.getLogger(SimulationService.class);

        private static final Duration ENTRY_INTERVAL = Duration.ofSeconds(2);
        private static final Duration EXIT_INTERVAL = Duration.ofSeconds(2);

        private static final int SLOTS_PER_LOCATION = 10;

        private static final int MAX_EXIT_ATTEMPTS = 3;

        private static final Set<String> ADOPTABLE_SESSION_STATUSES = Set.of("RESERVED", "CREATED", "ACTIVE");

        private static final List<String> INDIAN_STATES = List.of(
                        "MH", "DL", "KA", "TN", "GJ", "RJ", "UP", "WB", "AP", "KL", "MP", "HR", "PB", "TS");
        private static final List<String> VEHICLE_BRANDS = List.of(
                        "Maruti", "Hyundai", "Tata", "Mahindra", "Honda", "Toyota", "Kia", "MG");
        private static final List<String> VEHICLE_MODELS = List.of(
                        "Swift", "i20", "Nexon", "XUV700", "City", "Innova", "Seltos", "Hector");
        private static final List<String> COLORS = List.of(
                        "White", "Black", "Silver", "Red", "Blue", "Grey");
        private static final String LETTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";

        private final VehicleService vehicleService;
        private final ParkingSlotService parkingSlotService;
        private final ParkingSessionService parkingSessionService;
        private final CameraEventPublisher cameraEventPublisher;
        private final PaymentMethodRepository paymentMethodRepository;
        private final ParkingSlotRepository parkingSlotRepository;
        private final GateRepository gateRepository;
        private final LocationRepository locationRepository;
        private final UserRepository userRepository;
        private final VehicleTypeService vehicleTypeService;
        private final ReservationClassService reservationClassService;

        private final Sinks.Many<SimulationEventDto> eventSink = Sinks.many().multicast().onBackpressureBuffer();
        private final Random random = new Random();

        private final AtomicBoolean simulationRunning = new AtomicBoolean(false);
        private final AtomicBoolean initializing = new AtomicBoolean(false);

        private final ConcurrentHashMap<Integer, SeenVehicle> vehiclesInside = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, String> slotStatus = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Integer> userActiveSession = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Integer> userVehicle = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, String> vehiclePlates = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, Integer> exitAttempts = new ConcurrentHashMap<>();
        private final Set<String> generatedVehicleNumbers = ConcurrentHashMap.newKeySet();

        private record SeenVehicle(
                        Integer vehicleId,
                        String licensePlate,
                        String userId,
                        String parkingLotId,
                        LocalDateTime seenAt) {
        }

        private volatile List<LocationDto> cachedLocations = List.of();
        private volatile List<UserDto> cachedUsers = List.of();
        private volatile List<Integer> cachedGateIds = List.of();
        private volatile List<Integer> cachedVehicleTypeIds = List.of();
        private volatile List<Integer> cachedPaymentMethodIds = List.of();
        private volatile SimulationRunContext currentSimulationContext;

        private volatile Disposable entryLoop;
        private volatile Disposable exitLoop;

        public SimulationService(
                        VehicleService vehicleService,
                        ParkingSlotService parkingSlotService,
                        ParkingSessionService parkingSessionService,
                        CameraEventPublisher cameraEventPublisher,
                        PaymentMethodRepository paymentMethodRepository,
                        ParkingSlotRepository parkingSlotRepository,
                        GateRepository gateRepository,
                        LocationRepository locationRepository,
                        UserRepository userRepository,
                        VehicleTypeService vehicleTypeService,
                        ReservationClassService reservationClassService) {

                this.vehicleService = vehicleService;
                this.parkingSlotService = parkingSlotService;
                this.parkingSessionService = parkingSessionService;
                this.cameraEventPublisher = cameraEventPublisher;
                this.paymentMethodRepository = paymentMethodRepository;
                this.parkingSlotRepository = parkingSlotRepository;
                this.gateRepository = gateRepository;
                this.locationRepository = locationRepository;
                this.userRepository = userRepository;
                this.vehicleTypeService = vehicleTypeService;
                this.reservationClassService = reservationClassService;
        }

        public Mono<SimulationRunResponseDto> startSimulation() {
                return Mono.defer(() -> {
                        if (!simulationRunning.compareAndSet(false, true)) {
                                SimulationRunContext running = currentSimulationContext;
                                return Mono.just(buildRunResponse("RUNNING",
                                                running != null ? running : new SimulationRunContext()));
                        }

                        SimulationRunContext context = new SimulationRunContext();
                        currentSimulationContext = context;
                        resetSimulationState();

                        LOGGER.info("Starting continuous parking simulation");
                        emitEvent(context, "SIMULATION_STARTED", "Simulation started", null);

                        return initialize(context)
                                        .onErrorResume(error -> {
                                                LOGGER.error("Simulation initialisation failed; loops will retry",
                                                                error);
                                                emitEvent(context, "SIMULATION_INIT_FAILED", error.getMessage(), null);
                                                return Mono.empty();
                                        })
                                        .then(Mono.fromRunnable(() -> {
                                                entryLoop = startLoop("entry", ENTRY_INTERVAL,
                                                                () -> entryTick(context));
                                                exitLoop = startLoop("exit", EXIT_INTERVAL, () -> exitTick(context));
                                                emitEvent(context, "SIMULATION_RUNNING",
                                                                "Entry loop every 500ms, exit loop every 2s; running until stopped",
                                                                null);
                                        }))
                                        .thenReturn(buildRunResponse("STARTED", context));
                });
        }

        public Mono<SimulationRunResponseDto> stopSimulation() {
                return Mono.fromSupplier(() -> {
                        SimulationRunContext context = currentSimulationContext;

                        if (!simulationRunning.compareAndSet(true, false)) {
                                return buildRunResponse("NOT_RUNNING",
                                                context != null ? context : new SimulationRunContext());
                        }

                        disposeLoops();
                        LOGGER.info("Simulation stopped");
                        if (context != null) {
                                emitEvent(context, "SIMULATION_STOPPED", "Simulation stopped by request", null);
                        }
                        return buildRunResponse("STOPPED", context != null ? context : new SimulationRunContext());
                });
        }

        public boolean isRunning() {
                return simulationRunning.get();
        }

        public Flux<SimulationEventDto> getEventStream() {
                return eventSink.asFlux();
        }

        @PreDestroy
        void shutdown() {
                simulationRunning.set(false);
                disposeLoops();
        }

        private void disposeLoops() {
                dispose(entryLoop);
                dispose(exitLoop);
                entryLoop = null;
                exitLoop = null;
        }

        private static void dispose(Disposable loop) {
                if (loop != null && !loop.isDisposed()) {
                        loop.dispose();
                }
        }

        private Disposable startLoop(String name, Duration interval, java.util.function.Supplier<Mono<Void>> tick) {
                return Flux.interval(interval, interval, Schedulers.parallel())
                                .onBackpressureDrop(dropped -> LOGGER.debug(
                                                "{} loop dropped tick {} (previous tick still running)", name,
                                                dropped))
                                .concatMap(tickNumber -> Mono.defer(tick::get)
                                                .onErrorResume(error -> {
                                                        LOGGER.error("{} loop tick {} failed; continuing", name,
                                                                        tickNumber, error);
                                                        return Mono.empty();
                                                }))
                                .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, interval)
                                                .doBeforeRetry(signal -> LOGGER.error(
                                                                "{} loop resubscribing after an unexpected terminal signal",
                                                                name, signal.failure())))
                                .subscribe(
                                                ignored -> {
                                                },
                                                error -> LOGGER.error("{} loop terminated unexpectedly", name, error),
                                                () -> LOGGER.info("{} loop completed", name));
        }

        private Mono<Void> entryTick(SimulationRunContext context) {
                if (!simulationRunning.get()) {
                        return Mono.empty();
                }

                if (cachedUsers.isEmpty() || slotStatus.isEmpty() || cachedPaymentMethodIds.isEmpty()) {
                        return reinitializeQuietly(context);
                }

                Optional<Integer> claimedSlot = claimAvailableSlot();
                if (claimedSlot.isEmpty()) {
                        LOGGER.debug("Entry tick idle: all {} slots occupied", slotStatus.size());
                        return Mono.empty();
                }

                Integer slotId = claimedSlot.get();
                Optional<UserDto> freeUser = claimFreeUser();
                if (freeUser.isEmpty()) {
                        releaseSlot(slotId);
                        LOGGER.debug("Entry tick idle: no user without an active session");
                        return Mono.empty();
                }

                UserDto user = freeUser.get();

                return resolveVehicle(user.userId())
                                .switchIfEmpty(Mono.defer(() -> {
                                        releaseSlot(slotId);
                                        userActiveSession.remove(user.userId());
                                        emitEvent(context, "VEHICLE_UNAVAILABLE",
                                                        "Could not resolve a vehicle for user " + user.userId(), null);
                                        return Mono.empty();
                                }))
                                .flatMap(vehicle -> reportEntry(context, user, vehicle, slotId))
                                .onErrorResume(error -> {
                                        releaseSlot(slotId);
                                        userActiveSession.remove(user.userId());
                                        LOGGER.error("Entry observation failed for user {} on slot {}",
                                                        user.userId(), slotId, error);
                                        emitEvent(context, "VEHICLE_ENTRY_FAILED", error.getMessage(), null);
                                        return Mono.empty();
                                })
                                .then();
        }

        private Mono<CameraEvent> reportEntry(
                        SimulationRunContext context,
                        UserDto user,
                        SimulatedVehicle vehicle,
                        Integer slotId) {

                String parkingLotId = randomLocationId();

                CameraEvent event = CameraEvent.of(
                                CameraEventType.CAR_ENTERED,
                                vehicle.licensePlate(),
                                vehicle.vehicleId(),
                                user.userId(),
                                parkingLotId,
                                entryCameraId(parkingLotId));

                return cameraEventPublisher.recordAndPublish(event)
                                .doOnSuccess(published -> {
                                        vehiclesInside.put(vehicle.vehicleId(), new SeenVehicle(
                                                        vehicle.vehicleId(),
                                                        vehicle.licensePlate(),
                                                        user.userId(),
                                                        parkingLotId,
                                                        LocalDateTime.now()));
                                        userActiveSession.put(user.userId(), vehicle.vehicleId());
                                        context.vehicleEntries.incrementAndGet();
                                        emitEvent(context, "CAMERA_CAR_ENTERED",
                                                        String.format(
                                                                        "Camera %s read %s entering %s (vehicle=%d user=%s)",
                                                                        published.cameraId(), vehicle.licensePlate(),
                                                                        parkingLotId, vehicle.vehicleId(),
                                                                        user.userId()),
                                                        null);
                                });
        }

        private Optional<Integer> claimAvailableSlot() {
                for (Map.Entry<Integer, String> entry : slotStatus.entrySet()) {
                        if (ParkingLifecycleService.SLOT_AVAILABLE.equals(entry.getValue())
                                        && slotStatus.replace(entry.getKey(),
                                                        ParkingLifecycleService.SLOT_AVAILABLE,
                                                        ParkingLifecycleService.SLOT_OCCUPIED)) {
                                return Optional.of(entry.getKey());
                        }
                }
                return Optional.empty();
        }

        private void releaseSlot(Integer slotId) {
                slotStatus.put(slotId, ParkingLifecycleService.SLOT_AVAILABLE);
        }

        private void releaseOneSlot() {
                for (Map.Entry<Integer, String> entry : slotStatus.entrySet()) {
                        if (ParkingLifecycleService.SLOT_OCCUPIED.equals(entry.getValue())
                                        && slotStatus.replace(entry.getKey(),
                                                        ParkingLifecycleService.SLOT_OCCUPIED,
                                                        ParkingLifecycleService.SLOT_AVAILABLE)) {
                                return;
                        }
                }
        }

        private String entryCameraId(String parkingLotId) {
                return "CAM-" + parkingLotId + "-ENTRY-" + (random.nextInt(2) + 1);
        }

        private String exitCameraId(String parkingLotId) {
                return "CAM-" + parkingLotId + "-EXIT-" + (random.nextInt(2) + 1);
        }

        private String randomLocationId() {
                return cachedLocations.isEmpty() ? null : randomValue(cachedLocations).locationId();
        }

        private Optional<UserDto> claimFreeUser() {
                for (UserDto user : cachedUsers) {
                        if (user.userId() != null
                                        && "ACTIVE".equalsIgnoreCase(user.userStatus())
                                        && userActiveSession.putIfAbsent(user.userId(), PENDING_SESSION) == null) {
                                return Optional.of(user);
                        }
                }
                return Optional.empty();
        }

        private static final Integer PENDING_SESSION = -1;

        private record SimulatedVehicle(Integer vehicleId, String licensePlate) {
        }

        private Mono<SimulatedVehicle> resolveVehicle(String userId) {
                Integer cachedId = userVehicle.get(userId);
                String cachedPlate = cachedId == null ? null : vehiclePlates.get(cachedId);
                if (cachedId != null && cachedPlate != null) {
                        return Mono.just(new SimulatedVehicle(cachedId, cachedPlate));
                }

                return vehicleService.getVehiclesByUserId(userId)
                                .next()
                                .map(vehicle -> new SimulatedVehicle(vehicle.vehicleId(), vehicle.vehicleNumber()))
                                .onErrorResume(error -> Mono.empty())
                                .switchIfEmpty(Mono.defer(() -> registerVehicleFor(userId)))
                                .filter(vehicle -> vehicle.licensePlate() != null && vehicle.vehicleId() != null)
                                .doOnNext(vehicle -> {
                                        userVehicle.put(userId, vehicle.vehicleId());
                                        vehiclePlates.put(vehicle.vehicleId(), vehicle.licensePlate());
                                });
        }

        private Mono<SimulatedVehicle> registerVehicleFor(String userId) {
                if (cachedVehicleTypeIds.isEmpty()) {
                        return Mono.empty();
                }

                String plate = generateNumberPlate();
                LocalDateTime now = LocalDateTime.now();
                VehicleDto vehicle = new VehicleDto(
                                null,
                                userId,
                                plate,
                                randomValue(cachedVehicleTypeIds),
                                randomValue(VEHICLE_BRANDS),
                                randomValue(VEHICLE_MODELS),
                                randomValue(COLORS),
                                true,
                                now,
                                now);

                return vehicleService.registerVehicle(vehicle)
                                .then(vehicleService.getVehiclesByUserId(userId)
                                                .filter(registered -> plate
                                                                .equalsIgnoreCase(registered.vehicleNumber()))
                                                .next()
                                                .map(registered -> new SimulatedVehicle(
                                                                registered.vehicleId(), registered.vehicleNumber())));
        }

        private String generateNumberPlate() {
                while (true) {
                        String plate = String.format(
                                        "%s%02d%c%c%04d",
                                        randomValue(INDIAN_STATES),
                                        random.nextInt(99) + 1,
                                        LETTERS.charAt(random.nextInt(26)),
                                        LETTERS.charAt(random.nextInt(26)),
                                        random.nextInt(10000));
                        if (generatedVehicleNumbers.add(plate)) {
                                return plate;
                        }
                }
        }

        private Mono<Void> exitTick(SimulationRunContext context) {
                if (!simulationRunning.get()) {
                        return Mono.empty();
                }

                Optional<SeenVehicle> oldest = vehiclesInside.values().stream()
                                .min(Comparator.comparing(SeenVehicle::seenAt));

                if (oldest.isEmpty()) {
                        LOGGER.debug("Exit tick idle: no car is inside");
                        return Mono.empty();
                }

                SeenVehicle seen = oldest.get();
                if (vehiclesInside.remove(seen.vehicleId()) == null) {
                        return Mono.empty();
                }

                String parkingLotId = seen.parkingLotId() != null ? seen.parkingLotId() : randomLocationId();

                CameraEvent event = CameraEvent.of(
                                CameraEventType.CAR_EXITED,
                                seen.licensePlate(),
                                seen.vehicleId(),
                                seen.userId(),
                                parkingLotId,
                                exitCameraId(parkingLotId));

                return cameraEventPublisher.recordAndPublish(event)
                                .doOnSuccess(published -> {
                                        releaseOneSlot();
                                        userActiveSession.remove(seen.userId());
                                        exitAttempts.remove(seen.vehicleId());
                                        context.vehicleExits.incrementAndGet();
                                        emitEvent(context, "CAMERA_CAR_EXITED",
                                                        String.format(
                                                                        "Camera %s read %s leaving %s (vehicle=%d user=%s)",
                                                                        published.cameraId(), seen.licensePlate(),
                                                                        seen.parkingLotId(), seen.vehicleId(),
                                                                        seen.userId()),
                                                        null);
                                })
                                .onErrorResume(error -> {
                                        handleFailedExit(context, seen, error);
                                        return Mono.empty();
                                })
                                .then();
        }

        private void handleFailedExit(SimulationRunContext context, SeenVehicle seen, Throwable error) {
                int attempts = exitAttempts.merge(seen.vehicleId(), 1, Integer::sum);
                LOGGER.error("Exit observation failed for plate {} (attempt {}/{})",
                                seen.licensePlate(), attempts, MAX_EXIT_ATTEMPTS, error);

                if (attempts >= MAX_EXIT_ATTEMPTS) {
                        releaseOneSlot();
                        userActiveSession.remove(seen.userId());
                        exitAttempts.remove(seen.vehicleId());
                        emitEvent(context, "CAMERA_EXIT_ABANDONED",
                                        String.format("Plate %s abandoned after %d failed exit observations",
                                                        seen.licensePlate(), attempts),
                                        null);
                        return;
                }

                vehiclesInside.put(seen.vehicleId(), seen);
                emitEvent(context, "CAMERA_EXIT_FAILED", error.getMessage(), null);
        }

        private void resetSimulationState() {
                vehiclesInside.clear();
                vehiclePlates.clear();
                slotStatus.clear();
                userActiveSession.clear();
                userVehicle.clear();
                exitAttempts.clear();
                generatedVehicleNumbers.clear();
        }

        private Mono<Void> reinitializeQuietly(SimulationRunContext context) {
                if (!initializing.compareAndSet(false, true)) {
                        return Mono.empty();
                }

                return initialize(context)
                                .onErrorResume(error -> {
                                        LOGGER.warn("Simulation data still unavailable: {}", error.getMessage());
                                        return Mono.empty();
                                })
                                .doFinally(signal -> initializing.set(false));
        }

        private Mono<Void> initialize(SimulationRunContext context) {
                return Mono.zip(
                                locationRepository.findAll().collectList(),
                                userRepository.findAll().collectList(),
                                gateRepository.findAll().collectList(),
                                paymentMethodRepository.findAll()
                                                .filter(method -> Boolean.TRUE.equals(method.isActive()))
                                                .collectList(),
                                vehicleTypeService.getAllVehicleTypes().collectList(),
                                reservationClassService.getAllReservationClasses().collectList(),
                                parkingSessionService.getAllSessionsFromDb().collectList())
                                .flatMap(loaded -> {
                                        cachedLocations = List.copyOf(loaded.getT1());
                                        cachedUsers = List.copyOf(loaded.getT2());
                                        cachedGateIds = loaded.getT3().stream()
                                                        .map(gate -> gate.gateId())
                                                        .filter(Objects::nonNull)
                                                        .toList();
                                        cachedPaymentMethodIds = loaded.getT4().stream()
                                                        .map(method -> method.methodId().intValue())
                                                        .toList();
                                        cachedVehicleTypeIds = loaded.getT5().stream()
                                                        .map(type -> type.vehicleTypeId())
                                                        .filter(Objects::nonNull)
                                                        .toList();
                                        List<Integer> reservationClassIds = loaded.getT6().stream()
                                                        .map(reservationClass -> reservationClass.classId())
                                                        .filter(Objects::nonNull)
                                                        .toList();

                                        return adoptExistingSessions(loaded.getT7())
                                                        .doOnSuccess(ignored -> LOGGER.info(
                                                                        "Loaded {} locations, {} users, {} gates, {} payment methods, {} adopted sessions",
                                                                        cachedLocations.size(), cachedUsers.size(),
                                                                        cachedGateIds.size(),
                                                                        cachedPaymentMethodIds.size(),
                                                                        vehiclesInside.size()))
                                                        .then(ensureSlots(context, reservationClassIds));
                                })
                                .doOnSuccess(ignored -> emitEvent(context, "SIMULATION_DATA_READY",
                                                String.format("%d slots, %d users, %d already inside",
                                                                slotStatus.size(), cachedUsers.size(),
                                                                vehiclesInside.size()),
                                                null))
                                .then();
        }

        private Mono<Void> adoptExistingSessions(List<ParkingSessionDto> sessions) {
                return Flux.fromIterable(sessions)
                                .filter(session -> session.sessionId() != null
                                                && session.vehicleId() != null
                                                && session.sessionStatus() != null
                                                && ADOPTABLE_SESSION_STATUSES
                                                                .contains(session.sessionStatus().toUpperCase()))
                                .flatMap(session -> vehicleService.getVehicleById(session.vehicleId())
                                                .onErrorResume(error -> Mono.empty())
                                                .filter(vehicle -> vehicle.vehicleNumber() != null)
                                                .doOnNext(vehicle -> {
                                                        vehiclePlates.put(session.vehicleId(), vehicle.vehicleNumber());
                                                        vehiclesInside.put(session.vehicleId(), new SeenVehicle(
                                                                        session.vehicleId(),
                                                                        vehicle.vehicleNumber(),
                                                                        session.userId(),
                                                                        null,
                                                                        session.actualEntryTime() != null
                                                                                        ? session.actualEntryTime()
                                                                                        : LocalDateTime.now()));
                                                        if (session.userId() != null) {
                                                                userActiveSession.put(session.userId(),
                                                                                session.vehicleId());
                                                        }
                                                }))
                                .then();
        }

        private Mono<Void> ensureSlots(SimulationRunContext context, List<Integer> reservationClassIds) {
                if (cachedLocations.isEmpty()) {
                        emitEvent(context, "NO_LOCATIONS", "No locations found in database", null);
                        return Mono.empty();
                }
                if (cachedVehicleTypeIds.isEmpty() || reservationClassIds.isEmpty()) {
                        emitEvent(context, "NO_SLOT_REFERENCE_DATA",
                                        "No vehicle types or reservation classes; cannot create slots", null);
                        return Mono.empty();
                }

                return parkingSlotRepository.findAll().collectList()
                                .flatMap(existing -> {
                                        Map<String, List<ParkingSlotDto>> byLocation = new ConcurrentHashMap<>();
                                        for (ParkingSlotDto slot : existing) {
                                                if (slot.locationId() != null) {
                                                        byLocation.computeIfAbsent(slot.locationId(),
                                                                        key -> Collections.synchronizedList(
                                                                                        new ArrayList<>()))
                                                                        .add(slot);
                                                }
                                        }

                                        List<ParkingSlotDto> toCreate = new ArrayList<>();
                                        for (LocationDto location : cachedLocations) {
                                                List<ParkingSlotDto> slots = byLocation.getOrDefault(
                                                                location.locationId(), List.of());

                                                seedSlotStatus(slots);

                                                int missing = SLOTS_PER_LOCATION - slots.size();
                                                if (missing <= 0) {
                                                        continue;
                                                }

                                                String suffix = location.locationId()
                                                                .substring(Math.max(0,
                                                                                location.locationId().length() - 3))
                                                                .toUpperCase();
                                                int startSequence = highestSequence(slots);

                                                for (int index = 1; index <= missing; index++) {
                                                        int sequence = startSequence + index;
                                                        toCreate.add(new ParkingSlotDto(
                                                                        null,
                                                                        location.locationId(),
                                                                        String.format("%s-%03d", suffix, sequence),
                                                                        randomValue(cachedVehicleTypeIds),
                                                                        randomValue(reservationClassIds),
                                                                        String.format("SENSOR-%s-%06d", suffix,
                                                                                        sequence),
                                                                        ParkingLifecycleService.SLOT_AVAILABLE));
                                                }
                                        }

                                        if (toCreate.isEmpty()) {
                                                emitEvent(context, "SLOTS_READY",
                                                                String.format("Reusing %d existing slots",
                                                                                slotStatus.size()),
                                                                null);
                                                return Mono.<Void>empty();
                                        }

                                        return parkingSlotService.createSlots(toCreate)
                                                        .then(parkingSlotRepository.findAll().collectList())
                                                        .doOnNext(this::seedSlotStatus)
                                                        .doOnNext(all -> emitEvent(context, "SLOTS_READY",
                                                                        String.format("%d slots available (%d newly created)",
                                                                                        slotStatus.size(),
                                                                                        toCreate.size()),
                                                                        null))
                                                        .then();
                                });
        }

        private void seedSlotStatus(List<ParkingSlotDto> slots) {
                for (ParkingSlotDto slot : slots) {
                        if (slot.slotId() == null) {
                                continue;
                        }
                        boolean free = ParkingLifecycleService.SLOT_AVAILABLE.equalsIgnoreCase(slot.currentStatus());
                        slotStatus.put(slot.slotId(), free
                                        ? ParkingLifecycleService.SLOT_AVAILABLE
                                        : ParkingLifecycleService.SLOT_OCCUPIED);
                }
        }

        private int highestSequence(List<ParkingSlotDto> slots) {
                int highest = 0;
                for (ParkingSlotDto slot : slots) {
                        String displayCode = slot.displayCode();
                        if (displayCode == null || !displayCode.contains("-")) {
                                continue;
                        }
                        String[] parts = displayCode.split("-");
                        if (parts.length != 2) {
                                continue;
                        }
                        try {
                                highest = Math.max(highest, Integer.parseInt(parts[1]));
                        } catch (NumberFormatException ignored) {
                        }
                }
                return highest;
        }

        private SimulationRunResponseDto buildRunResponse(String status, SimulationRunContext context) {
                String loopState = simulationRunning.get() ? "RUNNING" : "STOPPED";
                List<SimulationStateDto> states = List.of(
                                new SimulationStateDto("Entry Loop", "every 500ms", loopState),
                                new SimulationStateDto("Exit Loop", "every 2s", loopState),
                                new SimulationStateDto("Inside", "Vehicles",
                                                String.valueOf(vehiclesInside.size())),
                                new SimulationStateDto("Slots", "Available",
                                                String.valueOf(availableSlotCount())),
                                new SimulationStateDto("Entries", "Observed",
                                                String.valueOf(context.vehicleEntries.get())),
                                new SimulationStateDto("Exits", "Observed",
                                                String.valueOf(context.vehicleExits.get())),
                                new SimulationStateDto("Result", "Simulation", status));

                return new SimulationRunResponseDto(status, states, List.copyOf(context.events));
        }

        private long availableSlotCount() {
                return slotStatus.values().stream()
                                .filter(ParkingLifecycleService.SLOT_AVAILABLE::equals)
                                .count();
        }

        private void emitEvent(SimulationRunContext context, String type, String message, Integer sessionId) {
                SimulationEventDto event = new SimulationEventDto(type, message, sessionId, LocalDateTime.now());
                context.recordEvent(event);
                eventSink.tryEmitNext(event);
        }

        private <T> T randomValue(List<T> values) {
                return values.get(random.nextInt(values.size()));
        }

        private static final class SimulationRunContext {
                private static final int MAX_RETAINED_EVENTS = 200;

                private final List<SimulationEventDto> events = Collections.synchronizedList(new ArrayList<>());
                private final AtomicInteger vehicleEntries = new AtomicInteger();
                private final AtomicInteger vehicleExits = new AtomicInteger();

                private void recordEvent(SimulationEventDto event) {
                        synchronized (events) {
                                events.add(event);
                                while (events.size() > MAX_RETAINED_EVENTS) {
                                        events.remove(0);
                                }
                        }
                }
        }
}

package com.example.Metropark.BFF.service;

// import java.math.BigDecimal;
// import java.math.RoundingMode;
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
// import com.example.Metropark.event.Event;
// import com.example.Metropark.event.ParkingEventSink;
import com.example.Metropark.gate.repo.GateRepository;
import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.parking.dto.ParkingSessionDto;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.parking.service.ParkingLifecycleService;
// import com.example.Metropark.parking.service.ParkingLifecycleService.ParkedVehicle;
import com.example.Metropark.parking.service.ParkingSessionService;
import com.example.Metropark.parking.service.ParkingSlotService;
import com.example.Metropark.payments.repo.PaymentMethodRepository;
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

/**
 * A bank of ANPR cameras watching a live parking lot, running until told to
 * stop.
 *
 * <p>
 * This class parks nothing. It reads number plates and says so; everything that
 * follows - slot allocation, sessions, payments, Redis, PostgreSQL - happens in
 * {@link com.example.Metropark.camera.consumer.ParkingCameraConsumer} after
 * RabbitMQ delivers the observation. Calling
 * {@link ParkingLifecycleService} from here would put the simulator on a code
 * path no real camera has, which is exactly what this refactor removed.
 *
 * <p>
 * Two independent loops drive it:
 *
 * <ul>
 * <li><b>Entry</b>, every 500ms, reports AT MOST ONE plate entering.
 * <li><b>Exit</b>, every 2s, reports AT MOST ONE plate leaving.
 * </ul>
 *
 * <p>
 * Neither loop ever terminates. Not when no user is free, not when every slot
 * is occupied, not when nothing is parked, and not when a tick throws or Redis
 * or RabbitMQ is briefly unreachable. A tick that cannot do its work does
 * nothing and waits for the next one. This is enforced structurally rather than
 * by hoping no exception escapes:
 *
 * <ul>
 * <li>every tick body ends in {@code onErrorResume} to an empty Mono, so a
 * failed tick is indistinguishable from an idle one to the interval above it;
 * <li>the interval itself carries an unbounded {@code retryWhen}, so even an
 * error raised outside the tick body (or a dropped-tick overflow) resubscribes
 * instead of completing;
 * <li>{@code concatMap} keeps at most one tick of a loop in flight, so a slow
 * tick delays the next one rather than racing it.
 * </ul>
 *
 * <p>
 * Because entry runs 4x as often as exit, the lot fills, entry then idles on
 * "every known plate is already inside", and each exit frees exactly one car
 * that the next entry tick can readmit. The simulation has no finished state.
 *
 * <p>
 * One consequence of the camera architecture is worth stating plainly: a tick
 * succeeding means the OBSERVATION was recorded and published, not that a car
 * parked. Whether parking followed is decided downstream and is visible in
 * {@code camera_events.status}. The counters below therefore count camera
 * events, which is the only thing this class is in a position to know.
 */
@Service
public class SimulationService {

        private static final Logger LOGGER = LoggerFactory.getLogger(SimulationService.class);

        /** Entry tick: one vehicle in, at most, every 500ms. */
        private static final Duration ENTRY_INTERVAL = Duration.ofSeconds(2);
        /** Exit tick: one vehicle out, at most, every 2 seconds. */
        private static final Duration EXIT_INTERVAL = Duration.ofSeconds(2);

        /** Slots the simulation ensures exist per location. */
        private static final int SLOTS_PER_LOCATION = 10;

        /**
         * A plate the cameras keep failing to report must not wedge the exit loop.
         * After this many attempts the car is dropped from the simulator's view; the
         * session it left behind is drained by adoption on the next run.
         */
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
        /**
         * The ONLY way this class reaches the parking domain: by reporting what a
         * camera saw. There is deliberately no ParkingLifecycleService here.
         */
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

        /**
         * Cars the cameras have seen enter and not yet seen leave, keyed by vehicle
         * id.
         *
         * <p>
         * Keyed by VEHICLE, not by session: the simulator publishes an observation and
         * never learns what session id the consumer went on to create, so a
         * session-keyed map would have nothing to put in it. The consumer resolves the
         * session from the vehicle when the exit event arrives.
         */
        private final ConcurrentHashMap<Integer, SeenVehicle> vehiclesInside = new ConcurrentHashMap<>();
        /** The simulator's own view of occupancy, used only to bound how many cars it admits. */
        private final ConcurrentHashMap<Integer, String> slotStatus = new ConcurrentHashMap<>();
        /** A user's car may be inside at most once at a time. */
        private final ConcurrentHashMap<String, Integer> userActiveSession = new ConcurrentHashMap<>();
        /** Resolved vehicle per user, so entry does not re-query on every tick. */
        private final ConcurrentHashMap<String, Integer> userVehicle = new ConcurrentHashMap<>();
        /** Plate per vehicle id - the only thing a camera actually reads. */
        private final ConcurrentHashMap<Integer, String> vehiclePlates = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Integer, Integer> exitAttempts = new ConcurrentHashMap<>();
        private final Set<String> generatedVehicleNumbers = ConcurrentHashMap.newKeySet();

        /**
         * What a camera needs to report a car again on its way out: the plate it will
         * read, and who the car belongs to. {@code seenAt} makes the exit loop
         * first-in-first-out.
         */
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

        // ==================== Lifecycle ====================

        /**
         * Starts THE simulation. Calling it while one is already running is a no-op
         * that reports RUNNING - there is only ever one simulation.
         */
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

                        // The loops start regardless of whether initialisation succeeds.
                        // A failed load must not prevent the simulation from running - the
                        // entry tick retries initialisation on its own and the loops simply
                        // idle until data is there.
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

        /** The only thing, besides shutdown, that ends the loops. */
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

        /**
         * An interval that cannot die.
         *
         * <p>
         * {@code concatMap} serialises ticks; the tick body swallows its own errors;
         * and {@code retryWhen} resubscribes on anything that still escapes - an
         * overflow from {@code onBackpressureDrop}, or an error signalled by the
         * interval itself. Only {@code dispose()} ends it.
         */
        private Disposable startLoop(String name, Duration interval, java.util.function.Supplier<Mono<Void>> tick) {
                return Flux.interval(interval, interval, Schedulers.parallel())
                                .onBackpressureDrop(dropped -> LOGGER.debug(
                                                "{} loop dropped tick {} (previous tick still running)", name,
                                                dropped))
                                // Mono.defer, so that a tick which throws SYNCHRONOUSLY while
                                // assembling its pipeline becomes an error signal the
                                // onErrorResume below can absorb. Calling tick.get() directly
                                // lets such a throw propagate past it and tear down the loop.
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

        // ==================== Entry loop ====================

        /**
         * At most one vehicle parked. If no user or no slot is free, this does nothing
         * and returns - the loop simply waits for the next tick.
         */
        private Mono<Void> entryTick(SimulationRunContext context) {
                if (!simulationRunning.get()) {
                        return Mono.empty();
                }

                if (cachedUsers.isEmpty() || slotStatus.isEmpty() || cachedPaymentMethodIds.isEmpty()) {
                        // Data never loaded, or the database was down at start. Retry the
                        // load from inside the loop instead of giving up on the simulation.
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

                // switchIfEmpty is attached to resolveVehicle ALONE. Attaching it to
                // the whole chain instead made a successful observation - which
                // completes empty - run the "no vehicle" fallback, releasing the slot
                // and the user a moment after taking them, so one slot could be
                // counted against any number of cars at once.
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
                                // The claim must be undone or the slot and the user leak,
                                // and the lot would slowly starve itself of both.
                                releaseSlot(slotId);
                                userActiveSession.remove(user.userId());
                                LOGGER.error("Entry observation failed for user {} on slot {}",
                                                user.userId(), slotId, error);
                                emitEvent(context, "VEHICLE_ENTRY_FAILED", error.getMessage(), null);
                                return Mono.empty();
                        })
                        .then();
        }

        /**
         * Reports one plate entering, and stops there.
         *
         * <p>
         * {@code slotId} is NOT sent: a camera cannot see which bay a car will take,
         * so the consumer allocates one. The simulator holds the slot in its own
         * ledger purely to stop itself admitting more cars than the lot can hold.
         *
         * <p>
         * The vehicle is recorded as inside as soon as the observation is published,
         * not when parking succeeds - the simulator never learns that. A car whose
         * entry the consumer rejects therefore lingers here until the exit loop gives
         * up on it after {@link #MAX_EXIT_ATTEMPTS}.
         */
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

        /**
         * Atomically takes one AVAILABLE slot. {@code replace(k, AVAILABLE, OCCUPIED)}
         * is the compare-and-set that makes the claim safe against the exit loop
         * releasing slots concurrently.
         */
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

        /**
         * Gives one slot back to the simulator's ledger without saying which.
         *
         * <p>
         * On exit the simulator genuinely does not know which bay the car was in - the
         * consumer chose it and never reported back. Since the ledger exists only to
         * cap how many cars are admitted, any occupied entry will do; the real slot
         * status lives in Redis and PostgreSQL and is corrected there by the exit
         * event.
         */
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

        /** Cameras are named after the lot they watch, so the ids look plausible in the audit log. */
        private String entryCameraId(String parkingLotId) {
                return "CAM-" + parkingLotId + "-ENTRY-" + (random.nextInt(2) + 1);
        }

        private String exitCameraId(String parkingLotId) {
                return "CAM-" + parkingLotId + "-EXIT-" + (random.nextInt(2) + 1);
        }

        private String randomLocationId() {
                return cachedLocations.isEmpty() ? null : randomValue(cachedLocations).locationId();
        }

        /**
         * Takes one user who holds no session. {@code putIfAbsent} reserves them in the
         * same step, so a later tick cannot pick the same user while this entry is
         * still in flight. The sentinel is replaced with the real session id on
         * success and removed on failure.
         */
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

        /** Placeholder held in userActiveSession while an entry is in flight. */
        private static final Integer PENDING_SESSION = -1;

        /**
         * A car and the plate a camera would read off it.
         *
         * <p>
         * The plate is what makes this the camera architecture rather than a rename:
         * it is the only identifier that crosses the wire, so it has to be carried
         * here rather than resolved later from a vehicle id the camera never saw.
         */
        private record SimulatedVehicle(Integer vehicleId, String licensePlate) {
        }

        /**
         * The user's vehicle, reused across sessions. A user is only ever picked when
         * their car is outside, so it is free by construction and no "already parked"
         * check is needed.
         */
        private Mono<SimulatedVehicle> resolveVehicle(String userId) {
                Integer cachedId = userVehicle.get(userId);
                String cachedPlate = cachedId == null ? null : vehiclePlates.get(cachedId);
                if (cachedId != null && cachedPlate != null) {
                        return Mono.just(new SimulatedVehicle(cachedId, cachedPlate));
                }

                return vehicleService.getVehiclesByUserId(userId)
                                .next()
                                .map(vehicle -> new SimulatedVehicle(vehicle.vehicleId(), vehicle.vehicleNumber()))
                                // getVehiclesByUserId reports "no vehicles" as an error rather
                                // than an empty Flux; that is a normal case here, not a failure.
                                .onErrorResume(error -> Mono.empty())
                                // Mono.defer is required, not stylistic: switchIfEmpty
                                // evaluates its argument eagerly, so without it
                                // registerVehicleFor ran on EVERY tick and registered a
                                // brand new vehicle for users who already had one.
                                .switchIfEmpty(Mono.defer(() -> registerVehicleFor(userId)))
                                // A car with no readable plate is invisible to a camera, so it
                                // cannot take part in the simulation at all.
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
                                                .filter(registered -> plate.equalsIgnoreCase(registered.vehicleNumber()))
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

        // ==================== Exit loop ====================

        /**
         * At most one vehicle exited. If nothing is parked, this does nothing and
         * returns - the loop waits for the next tick.
         */
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
                // Remove first: the vehicle is now owned by this tick, so a later tick
                // cannot pick it again while the observation is in flight.
                if (vehiclesInside.remove(seen.vehicleId()) == null) {
                        return Mono.empty();
                }

                // A car adopted from a previous run was never seen entering, so no lot
                // was recorded for it. Fall back to a known one rather than emitting
                // "CAM-null-EXIT-1" - the consumer resolves the exit from the plate and
                // ignores the lot, but a null here makes the audit log unreadable.
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
                                        // The simulator frees its own bookkeeping as soon as the
                                        // observation is out. Whether the session actually closed
                                        // is the consumer's business and shows up in
                                        // camera_events.status, not here.
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

        /**
         * A car whose exit observation cannot be published is retried on later ticks,
         * but not forever: one that can never be reported would otherwise be re-picked
         * every 2 seconds and block every other car from leaving.
         */
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

        // ==================== Initialisation ====================

        private void resetSimulationState() {
                vehiclesInside.clear();
                vehiclePlates.clear();
                slotStatus.clear();
                userActiveSession.clear();
                userVehicle.clear();
                exitAttempts.clear();
                generatedVehicleNumbers.clear();
        }

        /**
         * Reloads reference data from inside the entry loop when the caches are empty.
         * Guarded so only one attempt runs at a time; a failure is logged and the tick
         * ends normally, leaving the loop to try again later.
         */
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

        /**
         * Cars left inside by a previous run are taken over so the exit loop can send
         * them back out. Without this their slots would stay occupied forever and the
         * lot would run permanently below capacity.
         *
         * <p>
         * Reactive, unlike the version this replaces, because adoption now needs the
         * number PLATE and a session row only carries a vehicle id. A camera reports
         * plates, so a car whose plate cannot be resolved cannot be reported leaving
         * and is deliberately not adopted - claiming it and then being unable to emit
         * its exit would occupy a user forever.
         */
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

        /**
         * Reuses the slots already in the database and tops each location up to
         * {@link #SLOTS_PER_LOCATION}. The previous implementation created a fresh
         * batch on every start, so the lot grew without bound across runs.
         */
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

        /**
         * Seeds the claim ledger from the database. A slot that is already OCCUPIED by
         * someone else stays that way; only genuinely free slots are offered to the
         * entry loop.
         */
        private void seedSlotStatus(List<ParkingSlotDto> slots) {
                // No cross-check against adopted cars any more: the simulator no longer
                // knows which slot any car is in, so the slot's own status - which the
                // lifecycle consumer keeps current - is the only thing to go on.
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
                                // display codes that are not "<suffix>-<number>" simply do not
                                // participate in sequence numbering
                        }
                }
                return highest;
        }

        // ==================== Reporting ====================

        private SimulationRunResponseDto buildRunResponse(String status, SimulationRunContext context) {
                String loopState = simulationRunning.get() ? "RUNNING" : "STOPPED";
                List<SimulationStateDto> states = List.of(
                                new SimulationStateDto("Entry Loop", "every 500ms", loopState),
                                new SimulationStateDto("Exit Loop", "every 2s", loopState),
                                new SimulationStateDto("Inside", "Vehicles",
                                                String.valueOf(vehiclesInside.size())),
                                new SimulationStateDto("Slots", "Available",
                                                String.valueOf(availableSlotCount())),
                                // "Observed", not "Parked". These count camera events published,
                                // which is all this class does. Reporting them as sessions or
                                // payments would claim an outcome only the consumer knows -
                                // camera_events.status is where that lives.
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
                /**
                 * Bounded: the simulation runs indefinitely, so an unbounded event list
                 * would be a slow memory leak. Only the most recent events are kept for
                 * the run-response snapshot; the SSE stream carries the full sequence.
                 */
                private static final int MAX_RETAINED_EVENTS = 200;

                private final List<SimulationEventDto> events = Collections.synchronizedList(new ArrayList<>());
                /**
                 * Camera events published, entry and exit. There are no payment counters:
                 * the simulator creates no payments, and keeping fields that could only
                 * ever read zero would misreport the pipeline as broken.
                 */
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

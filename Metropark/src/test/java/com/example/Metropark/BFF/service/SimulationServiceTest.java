package com.example.Metropark.BFF.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.example.Metropark.BFF.dto.SimulationRunResponseDto;
import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.event.CameraEventPublisher;
import com.example.Metropark.camera.event.CameraEventType;
import com.example.Metropark.gate.dto.GateDto;
import com.example.Metropark.gate.repo.GateRepository;
import com.example.Metropark.location.dto.LocationDto;
import com.example.Metropark.location.repo.LocationRepository;
import com.example.Metropark.parking.dto.ParkingSlotDto;
import com.example.Metropark.parking.repo.ParkingSlotRepository;
import com.example.Metropark.parking.service.ParkingSessionService;
import com.example.Metropark.parking.service.ParkingSlotService;
import com.example.Metropark.payments.dto.PaymentMethodDto;
import com.example.Metropark.payments.repo.PaymentMethodRepository;
import com.example.Metropark.reservation.dto.ReservationClassDto;
import com.example.Metropark.reservation.service.ReservationClassService;
import com.example.Metropark.user.dto.UserDto;
import com.example.Metropark.user.repo.UserRepository;
import com.example.Metropark.vehicle.dto.VehicleResponseDto;
import com.example.Metropark.vehicle.dto.VehicleTypeDto;
import com.example.Metropark.vehicle.service.VehicleService;
import com.example.Metropark.vehicle.service.VehicleTypeService;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

class SimulationServiceTest {

        private static final Duration OBSERVATION_WINDOW = Duration.ofMillis(2200);

        private VehicleService vehicleService;
        private ParkingSlotService parkingSlotService;
        private ParkingSessionService parkingSessionService;
        private CameraEventPublisher cameraEventPublisher;
        private PaymentMethodRepository paymentMethodRepository;
        private ParkingSlotRepository parkingSlotRepository;
        private GateRepository gateRepository;
        private LocationRepository locationRepository;
        private UserRepository userRepository;
        private VehicleTypeService vehicleTypeService;
        private ReservationClassService reservationClassService;

        private SimulationService service;

        private final ConcurrentLinkedQueue<CameraEvent> entered = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<CameraEvent> exited = new ConcurrentLinkedQueue<>();

        @BeforeEach
        void setUp() {
                vehicleService = mock(VehicleService.class);
                parkingSlotService = mock(ParkingSlotService.class);
                parkingSessionService = mock(ParkingSessionService.class);
                cameraEventPublisher = mock(CameraEventPublisher.class);
                paymentMethodRepository = mock(PaymentMethodRepository.class);
                parkingSlotRepository = mock(ParkingSlotRepository.class);
                gateRepository = mock(GateRepository.class);
                locationRepository = mock(LocationRepository.class);
                userRepository = mock(UserRepository.class);
                vehicleTypeService = mock(VehicleTypeService.class);
                reservationClassService = mock(ReservationClassService.class);

                service = new SimulationService(
                                vehicleService,
                                parkingSlotService,
                                parkingSessionService,
                                cameraEventPublisher,
                                paymentMethodRepository,
                                parkingSlotRepository,
                                gateRepository,
                                locationRepository,
                                userRepository,
                                vehicleTypeService,
                                reservationClassService);
        }

        @AfterEach
        void tearDown() {
                service.stopSimulation().block();
        }

        private void givenReferenceData(int slotCount, int userCount) {
                when(locationRepository.findAll())
                                .thenReturn(Flux.just(new LocationDto("LOC-001", 1, "Central", "Chennai", "ACTIVE")));
                when(gateRepository.findAll()).thenReturn(Flux.just(
                                new GateDto(1, "LOC-001", "Gate 1", "ENTRY", "ACTIVE", null, null)));
                when(paymentMethodRepository.findAll()).thenReturn(Flux.just(
                                new PaymentMethodDto(1L, "UPI", true, null)));
                when(vehicleTypeService.getAllVehicleTypes()).thenReturn(Flux.just(
                                new VehicleTypeDto(1, "Car")));
                when(reservationClassService.getAllReservationClasses()).thenReturn(Flux.just(
                                new ReservationClassDto(1, "General")));
                when(parkingSessionService.getAllSessionsFromDb()).thenReturn(Flux.empty());

                List<UserDto> users = java.util.stream.IntStream.rangeClosed(1, userCount)
                                .mapToObj(index -> new UserDto("USR-" + index, "User " + index,
                                                "u" + index + "@example.com", "900000000" + index, "ACTIVE", null))
                                .toList();
                when(userRepository.findAll()).thenReturn(Flux.fromIterable(users));

                List<ParkingSlotDto> slots = java.util.stream.IntStream.rangeClosed(1, slotCount)
                                .mapToObj(index -> new ParkingSlotDto(index, "LOC-001",
                                                String.format("001-%03d", index), 1, 1,
                                                "SENSOR-" + index, "AVAILABLE"))
                                .toList();
                when(parkingSlotRepository.findAll()).thenReturn(Flux.fromIterable(slots));

                when(vehicleService.getVehiclesByUserId(anyString())).thenAnswer(invocation -> {
                        String userId = invocation.getArgument(0);
                        int index = Integer.parseInt(userId.substring(4));
                        return Flux.just(new VehicleResponseDto(index, userId, "User", "TN01AB000" + index,
                                        1, "Car", "Tata", "Nexon", "White", true, null, null));
                });

                when(parkingSlotService.createSlots(any())).thenReturn(Mono.just(0));
        }

        private void givenCameraPublishSucceeds() {
                when(cameraEventPublisher.recordAndPublish(any(CameraEvent.class))).thenAnswer(invocation -> {
                        CameraEvent event = invocation.getArgument(0);
                        if (event.eventType() == CameraEventType.CAR_ENTERED) {
                                entered.add(event);
                        } else {
                                exited.add(event);
                        }
                        return Mono.just(event);
                });
        }

        private void givenCameraPublishFails(AtomicInteger attempts) {
                when(cameraEventPublisher.recordAndPublish(any(CameraEvent.class))).thenAnswer(invocation -> {
                        attempts.incrementAndGet();
                        return Mono.error(new IllegalStateException("rabbitmq is down"));
                });
        }

        @Test
        void startsOnceAndReportsRunningOnASecondCall() {
                givenReferenceData(4, 4);
                givenCameraPublishSucceeds();

                SimulationRunResponseDto first = service.startSimulation().block();
                SimulationRunResponseDto second = service.startSimulation().block();

                assertEquals("STARTED", first.status());
                assertEquals("RUNNING", second.status(), "a second start must not launch a second simulation");
                assertTrue(service.isRunning());
        }

        @Test
        void entryLoopParksOneVehiclePerTickAndStopsAtCapacity() throws Exception {
                givenReferenceData(1, 10);
                givenCameraPublishSucceeds();

                service.startSimulation().block();
                Thread.sleep(OBSERVATION_WINDOW.toMillis());

                assertTrue(entered.size() >= 1, "the free slot should have been filled");
                assertTrue(entered.size() <= 3,
                                "at most one vehicle per tick, bounded by the single slot; got " + entered.size());
        }

        @Test
        void loopKeepsRunningWhenNoSlotIsAvailable() throws Exception {
                givenReferenceData(0, 10);
                givenCameraPublishSucceeds();

                service.startSimulation().block();
                Thread.sleep(OBSERVATION_WINDOW.toMillis());

                assertTrue(entered.isEmpty(), "no slots means nothing parks");
                assertTrue(service.isRunning(), "the loop must idle, not finish, when the lot is full");
        }

        @Test
        void loopKeepsRunningWhenNoUserIsAvailable() throws Exception {
                givenReferenceData(5, 0);
                givenCameraPublishSucceeds();

                service.startSimulation().block();
                Thread.sleep(OBSERVATION_WINDOW.toMillis());

                assertTrue(entered.isEmpty(), "no users means nothing parks");
                assertTrue(service.isRunning(), "the loop must idle, not finish, when no user is free");
        }

        @Test
        void loopSurvivesTicksThatFail() throws Exception {
                givenReferenceData(5, 10);

                AtomicInteger attempts = new AtomicInteger();
                givenCameraPublishFails(attempts);

                service.startSimulation().block();
                Thread.sleep(OBSERVATION_WINDOW.toMillis());

                assertTrue(attempts.get() >= 2,
                                "the loop must keep ticking after a failure; attempts=" + attempts.get());
                assertTrue(service.isRunning(), "a failing tick must not stop the simulation");
        }

        @Test
        void failedEntryReleasesTheClaimedSlotAndUser() throws Exception {
                givenReferenceData(1, 1);

                AtomicInteger attempts = new AtomicInteger();
                givenCameraPublishFails(attempts);

                service.startSimulation().block();
                Thread.sleep(OBSERVATION_WINDOW.toMillis());

                assertTrue(attempts.get() >= 2,
                                "slot and user must be released after a failed entry; attempts=" + attempts.get());
        }

        @Test
        void exitLoopReleasesParkedVehicles() throws Exception {
                givenReferenceData(5, 5);
                givenCameraPublishSucceeds();

                service.startSimulation().block();
                Thread.sleep(Duration.ofMillis(3000).toMillis());

                assertFalse(entered.isEmpty(), "vehicles should have parked");
                assertFalse(exited.isEmpty(), "the exit loop should have released at least one vehicle");
                assertTrue(exited.size() <= 2,
                                "at most one exit per 2s tick; got " + exited.size());
        }

        @Test
        void stopEndsTheLoops() throws Exception {
                givenReferenceData(10, 10);
                givenCameraPublishSucceeds();

                service.startSimulation().block();
                Thread.sleep(1200);

                SimulationRunResponseDto stopped = service.stopSimulation().block();
                assertEquals("STOPPED", stopped.status());
                assertFalse(service.isRunning());

                int afterStop = entered.size();
                Thread.sleep(1200);
                assertEquals(afterStop, entered.size(), "no work may happen after stop");
        }
}

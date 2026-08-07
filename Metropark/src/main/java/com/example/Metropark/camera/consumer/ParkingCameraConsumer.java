package com.example.Metropark.camera.consumer;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.event.CameraEventStatus;
import com.example.Metropark.camera.repo.CameraEventRepository;
import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.parking.service.ParkingLifecycleService;
import com.example.Metropark.payments.repo.PaymentMethodRepository;
import com.example.Metropark.redis.DistributedLockService;
import com.example.Metropark.vehicle.repo.VehicleRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Mono;

/**
 * Turns a camera observation into a parking operation.
 *
 * <p>
 * This is the ONLY thing that starts a park or an exit. Nothing calls
 * {@link ParkingLifecycleService} on the producing side any more, so a car is
 * parked because a camera saw it and the broker delivered that fact - not
 * because a scheduler decided to.
 *
 * <p>
 * All parking logic stays in {@link ParkingLifecycleService}. This class only
 * resolves what the camera could not know (which vehicle, which slot, which
 * session) and enforces exactly-once per plate.
 *
 * <p>
 * Acknowledgement follows the same contract as
 * {@link com.example.Metropark.event.consumer.EventConsumer}: the factory is
 * MANUAL, so every delivery settles here, after the reactive work finishes.
 */
@Component
public class ParkingCameraConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingCameraConsumer.class);

    private static final String CURRENCY = "INR";

    private final ParkingLifecycleService parkingLifecycleService;
    private final VehicleRepository vehicleRepository;
    private final PaymentMethodRepository paymentMethodRepository;
    private final DistributedLockService lockService;
    private final CameraEventRepository cameraEventRepository;
    private final ObjectMapper objectMapper;

    private final Random random = new Random();

    public ParkingCameraConsumer(
            ParkingLifecycleService parkingLifecycleService,
            VehicleRepository vehicleRepository,
            PaymentMethodRepository paymentMethodRepository,
            DistributedLockService lockService,
            CameraEventRepository cameraEventRepository,
            ObjectMapper objectMapper) {

        this.parkingLifecycleService = parkingLifecycleService;
        this.vehicleRepository = vehicleRepository;
        this.paymentMethodRepository = paymentMethodRepository;
        this.lockService = lockService;
        this.cameraEventRepository = cameraEventRepository;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.CAMERA_PROCESSING_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consume(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        CameraEvent event = parse(message);
        if (event == null || event.licensePlate() == null || event.eventType() == null) {
            deadLetter(channel, deliveryTag, "unreadable camera event");
            return;
        }

        // The lock is per PLATE, not per event: the duplicate we are guarding
        // against is the same car read twice with two different event ids, which
        // an event-id check would sail straight past.
        lockService.acquirePlateLock(event.licensePlate())
                .flatMap(lock -> apply(event)
                        .doFinally(signal -> lockService.release(lock).subscribe()))
                // Empty means the lock was held: another delivery is mid-flight for
                // this plate. Ack it - it is a duplicate observation, which is
                // normal for ANPR, not a failure to retry or dead letter.
                .switchIfEmpty(Mono.fromRunnable(() -> LOGGER.debug(
                        "Ignoring duplicate camera event {} for plate {}: already in flight",
                        event.eventId(), event.licensePlate())))
                .subscribe(
                        ignored -> {
                        },
                        error -> {
                            LOGGER.error("Camera event {} ({}) failed for plate {}",
                                    event.eventId(), event.eventType(), event.licensePlate(), error);
                            // Recorded before the nack so the audit row explains the dead
                            // letter. Best effort: if the audit write itself fails the
                            // message must still be dead lettered, never left unsettled.
                            cameraEventRepository
                                    .updateStatus(event.eventId(), CameraEventStatus.FAILED, error.getMessage())
                                    .onErrorResume(auditError -> {
                                        LOGGER.error("Failed to record FAILED for camera event {}",
                                                event.eventId(), auditError);
                                        return Mono.empty();
                                    })
                                    .doFinally(signal -> deadLetter(
                                            channel, deliveryTag, "processing failed for " + event.licensePlate()))
                                    .subscribe();
                        },
                        () -> cameraEventRepository
                                .updateStatus(event.eventId(), CameraEventStatus.PROCESSED)
                                .onErrorResume(auditError -> {
                                    LOGGER.error("Failed to record PROCESSED for camera event {}",
                                            event.eventId(), auditError);
                                    return Mono.empty();
                                })
                                .doFinally(signal -> ack(channel, deliveryTag))
                                .subscribe());
    }

    /**
     * Resolves the plate to a registered vehicle, then hands off. An unknown plate
     * is a real failure: a car the system cannot identify entered or left, and that
     * belongs in the dead letter queue rather than being silently dropped.
     */
    private Mono<Void> apply(CameraEvent event) {
        return vehicleRepository.findByVehicleNumber(event.licensePlate())
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "No vehicle registered for plate " + event.licensePlate())))
                .flatMap(vehicle -> event.isEntry()
                        ? park(event, vehicle.vehicleId(), vehicle.userId())
                        : parkingLifecycleService.exitVehicleByVehicleId(vehicle.vehicleId()));
    }

    private Mono<Void> park(CameraEvent event, Integer vehicleId, String ownerUserId) {
        // The vehicle's registered owner wins over the userId the event carries:
        // the event's copy is whatever the producer believed, while the vehicle
        // row is what the database will enforce.
        String userId = ownerUserId != null ? ownerUserId : event.userId();

        return activePaymentMethodIds()
                .flatMap(methodIds -> parkingLifecycleService.parkVehicleAtLot(
                        userId,
                        vehicleId,
                        event.parkingLotId(),
                        null,
                        methodIds.get(random.nextInt(methodIds.size())),
                        randomAmount(),
                        CURRENCY))
                .then();
    }

    private Mono<List<Integer>> activePaymentMethodIds() {
        return paymentMethodRepository.findAll()
                .filter(method -> Boolean.TRUE.equals(method.isActive()))
                .map(method -> method.methodId().intValue())
                .collectList()
                .filter(ids -> !ids.isEmpty())
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "Cannot park: no active payment method exists.")));
    }

    private BigDecimal randomAmount() {
        return BigDecimal.valueOf(10.0 + random.nextDouble() * 40.0).setScale(2, RoundingMode.HALF_UP);
    }

    private CameraEvent parse(Message message) {
        try {
            return objectMapper.readValue(message.getBody(), CameraEvent.class);
        } catch (Exception e) {
            LOGGER.error("Failed to parse camera event", e);
            return null;
        }
    }

    private void ack(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOGGER.error("Failed to ack delivery {}", deliveryTag, e);
        }
    }

    private void deadLetter(Channel channel, long deliveryTag, String what) {
        LOGGER.error("Dead lettering camera delivery {} ({})", deliveryTag, what);
        try {
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            LOGGER.error("Failed to nack delivery {}", deliveryTag, e);
        }
    }
}

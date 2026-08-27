package com.example.Metropark.camera.consumer;

import java.math.BigDecimal;
import java.math.RoundingMode;
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
import com.example.Metropark.redis.DistributedLockService;
import com.example.Metropark.vehicle.repo.VehicleRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Mono;

@Component
public class ParkingCameraConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParkingCameraConsumer.class);

    private static final String CURRENCY = "INR";

    private final ParkingLifecycleService parkingLifecycleService;
    private final VehicleRepository vehicleRepository;
    private final DistributedLockService lockService;
    private final CameraEventRepository cameraEventRepository;
    private final ObjectMapper objectMapper;

    private final Random random = new Random();

    public ParkingCameraConsumer(
            ParkingLifecycleService parkingLifecycleService,
            VehicleRepository vehicleRepository,
            DistributedLockService lockService,
            CameraEventRepository cameraEventRepository,
            ObjectMapper objectMapper) {

        this.parkingLifecycleService = parkingLifecycleService;
        this.vehicleRepository = vehicleRepository;
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

        lockService.acquirePlateLock(event.licensePlate())
                .flatMap(lock -> apply(event)
                        .doFinally(signal -> lockService.release(lock).subscribe()))
                .switchIfEmpty(Mono.fromRunnable(() -> LOGGER.debug(
                        "Ignoring duplicate camera event {} for plate {}: already in flight",
                        event.eventId(), event.licensePlate())))
                .subscribe(
                        ignored -> {
                        },
                        error -> {
                            LOGGER.error("Camera event {} ({}) failed for plate {}",
                                    event.eventId(), event.eventType(), event.licensePlate(), error);
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

    private Mono<Void> apply(CameraEvent event) {
        return vehicleRepository.findByVehicleNumber(event.licensePlate())
                .switchIfEmpty(Mono.error(new IllegalStateException(
                        "No vehicle registered for plate " + event.licensePlate())))
                .flatMap(vehicle -> event.isEntry()
                        ? park(event, vehicle.vehicleId(), vehicle.userId())
                        : parkingLifecycleService.exitVehicleByVehicleId(vehicle.vehicleId()));
    }

    private Mono<Void> park(CameraEvent event, Integer vehicleId, String ownerUserId) {
        String userId = ownerUserId != null ? ownerUserId : event.userId();

        return parkingLifecycleService.parkVehicleAtLot(
                userId,
                vehicleId,
                event.parkingLotId(),
                null,
                randomAmount(),
                CURRENCY)
                .then();
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

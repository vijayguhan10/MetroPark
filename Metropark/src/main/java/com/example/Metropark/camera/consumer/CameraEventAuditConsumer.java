package com.example.Metropark.camera.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.example.Metropark.camera.event.CameraEvent;
import com.example.Metropark.camera.repo.CameraEventRepository;
import com.example.Metropark.config.RabbitMQConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

/**
 * Keeps {@code camera_events.status} honest. Contains no parking logic and
 * touches no parking table.
 *
 * <p>
 * It reads its own copy of every camera event from
 * {@value RabbitMQConfig#CAMERA_AUDIT_QUEUE}, bound to the same topic exchange as
 * the processing queue. Because the two queues are independent, this consumer
 * has no idea whether parking succeeded - and must not guess. It records only
 * the one fact it can actually witness: the event reached the pipeline. The
 * terminal PROCESSED / FAILED verdict is written by
 * {@link ParkingCameraConsumer}, which is the only party that knows it.
 *
 * <p>
 * That split is why the update is conditional - see
 * {@link CameraEventRepository#markProcessingIfReceived}.
 */
@Component
public class CameraEventAuditConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(CameraEventAuditConsumer.class);

    private final CameraEventRepository cameraEventRepository;
    private final ObjectMapper objectMapper;

    public CameraEventAuditConsumer(CameraEventRepository cameraEventRepository, ObjectMapper objectMapper) {
        this.cameraEventRepository = cameraEventRepository;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.CAMERA_AUDIT_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consume(Message message, Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        CameraEvent event = parse(message);
        if (event == null || event.eventId() == null) {
            deadLetter(channel, deliveryTag, "unreadable camera event");
            return;
        }

        cameraEventRepository.markProcessingIfReceived(event.eventId())
                .subscribe(
                        rows -> {
                            if (rows == 0) {
                                LOGGER.debug("Camera event {} already past RECEIVED; audit left untouched",
                                        event.eventId());
                            }
                        },
                        error -> {
                            LOGGER.error("Failed to audit camera event {}", event.eventId(), error);
                            deadLetter(channel, deliveryTag, "audit failed for " + event.eventId());
                        },
                        () -> ack(channel, deliveryTag));
    }

    private CameraEvent parse(Message message) {
        try {
            return objectMapper.readValue(message.getBody(), CameraEvent.class);
        } catch (Exception e) {
            LOGGER.error("Failed to parse camera event for audit", e);
            return null;
        }
    }

    private void ack(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOGGER.error("Failed to ack audit delivery {}", deliveryTag, e);
        }
    }

    private void deadLetter(Channel channel, long deliveryTag, String what) {
        LOGGER.error("Dead lettering audit delivery {} ({})", deliveryTag, what);
        try {
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            LOGGER.error("Failed to nack audit delivery {}", deliveryTag, e);
        }
    }
}

package com.example.Metropark.event.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.Event;
import com.example.Metropark.event.payload.NotificationEventPayload;
import com.example.Metropark.payments.payment.service.EmailService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Mono;

@Component
public class NotificationConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NotificationConsumer.class);

    private final EmailService emailService;
    private final ObjectMapper objectMapper;

    public NotificationConsumer(EmailService emailService, ObjectMapper objectMapper) {
        this.emailService = emailService;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.NOTIFICATION_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumeNotification(
            Message message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);
        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable notification envelope");
            return;
        }

        NotificationEventPayload payload = parsePayload(event.payload(), NotificationEventPayload.class);
        if (payload == null || payload.sessionId() == null) {
            deadLetter(channel, deliveryTag, "notification event carries no session information");
            return;
        }

        LOGGER.info("NOTIFICATION CONSUMER RECEIVED | sessionId={} userId={} status={}",
                payload.sessionId(), payload.userId(), payload.status());

        emailService.sendNotificationEmail(payload, payload.userId())
                .subscribe(
                        ignored -> {
                        },
                        error -> {
                            LOGGER.error("Failed to send notification email for session {}",
                                    payload.sessionId(), error);
                            deadLetter(channel, deliveryTag,
                                    "notification email failed for session " + payload.sessionId());
                        },
                        () -> ack(channel, deliveryTag));
    }

    private void ack(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOGGER.error("Failed to ack notification delivery {}", deliveryTag, e);
        }
    }

    private void deadLetter(Channel channel, long deliveryTag, String reason) {
        LOGGER.error("Dead lettering notification delivery {} ({})", deliveryTag, reason);
        try {
            channel.basicNack(deliveryTag, false, false);
        } catch (Exception e) {
            LOGGER.error("Failed to nack notification delivery {}", deliveryTag, e);
        }
    }

    private Event parseEvent(Message message) {
        try {
            return objectMapper.readValue(message.getBody(), Event.class);
        } catch (Exception e) {
            LOGGER.error("Failed to parse notification event", e);
            return null;
        }
    }

    private <T> T parsePayload(Object payload, Class<T> clazz) {
        try {
            if (payload == null) {
                return null;
            }
            if (clazz.isInstance(payload)) {
                return clazz.cast(payload);
            }
            return objectMapper.convertValue(payload, clazz);
        } catch (Exception e) {
            LOGGER.error("Failed to parse payload as {}", clazz.getSimpleName(), e);
            return null;
        }
    }
}

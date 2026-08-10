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
import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
import com.example.Metropark.parking.repo.ParkingLifecycleRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;

import reactor.core.publisher.Mono;

@Component
public class EventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EventConsumer.class);

    private final ParkingLifecycleRepository lifecycleRepository;
    private final ObjectMapper objectMapper;

    public EventConsumer(
            ParkingLifecycleRepository lifecycleRepository,
            ObjectMapper objectMapper) {
        this.lifecycleRepository = lifecycleRepository;
        this.objectMapper = objectMapper;
    }

    @RabbitListener(queues = RabbitMQConfig.LIFECYCLE_EVENTS_QUEUE, containerFactory = "rabbitListenerContainerFactory")
    public void consumeLifecycleEvent(
            Message message,
            Channel channel,
            @Header(AmqpHeaders.DELIVERY_TAG) long deliveryTag) {

        Event event = parseEvent(message);

        if (event == null) {
            deadLetter(channel, deliveryTag, "unparseable lifecycle envelope");
            return;
        }

        ParkingLifecycleEventPayload payload = parsePayload(event.payload(), ParkingLifecycleEventPayload.class);

        if (payload == null || payload.session() == null) {
            deadLetter(
                    channel,
                    deliveryTag,
                    "lifecycle event " + event.eventId() + " carries no session");
            return;
        }

        Integer sessionId = payload.session().sessionId();

        Mono<ParkingLifecycleRepository.PersistResult> work = switch (event.type()) {

            case RabbitMQConfig.VEHICLE_ENTRY_KEY ->
                lifecycleRepository.persistEntry(payload);

            case RabbitMQConfig.VEHICLE_EXIT_KEY ->
                lifecycleRepository.persistExit(payload);

            default -> {
                LOGGER.warn(
                        "Ignoring unknown lifecycle event type {}",
                        event.type());
                yield Mono.empty();
            }
        };

        work.subscribe(
                result -> LOGGER.info(
                        "PERSISTED {} | session={} sessionRows={} slotRows={} paymentRows={}",
                        event.type(),
                        sessionId,
                        result.sessionRows(),
                        result.slotRows(),
                        result.paymentRows()),
                error -> {
                    LOGGER.error(
                            "Failed to persist {} for session {}",
                            event.type(),
                            sessionId,
                            error);

                    deadLetter(
                            channel,
                            deliveryTag,
                            "persist failed for session " + sessionId);
                },
                () -> ack(channel, deliveryTag));
    }

    private void ack(Channel channel, long deliveryTag) {
        try {
            channel.basicAck(deliveryTag, false);
        } catch (Exception e) {
            LOGGER.error(
                    "Failed to ack delivery {}",
                    deliveryTag,
                    e);
        }
    }

    private void deadLetter(
            Channel channel,
            long deliveryTag,
            String what) {

        LOGGER.error(
                "Dead lettering delivery {} ({})",
                deliveryTag,
                what);

        try {
            channel.basicNack(
                    deliveryTag,
                    false,
                    false);
        } catch (Exception e) {
            LOGGER.error(
                    "Failed to nack delivery {}",
                    deliveryTag,
                    e);
        }
    }

    private Event parseEvent(Message message) {
        try {
            return objectMapper.readValue(
                    message.getBody(),
                    Event.class);
        } catch (Exception e) {
            LOGGER.error(
                    "Failed to parse event from message",
                    e);
            return null;
        }
    }

    private <T> T parsePayload(
            Object payload,
            Class<T> clazz) {

        try {
            if (payload == null) {
                return null;
            }

            if (clazz.isInstance(payload)) {
                return clazz.cast(payload);
            }

            return objectMapper.convertValue(
                    payload,
                    clazz);

        } catch (Exception e) {
            LOGGER.error(
                    "Failed to parse payload as {}",
                    clazz.getSimpleName(),
                    e);
            return null;
        }
    }
}
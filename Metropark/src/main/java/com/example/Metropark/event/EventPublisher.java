package com.example.Metropark.event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import com.example.Metropark.config.RabbitMQConfig;
import com.example.Metropark.event.payload.ParkingLifecycleEventPayload;
import com.example.Metropark.event.payload.PaymentEventPayload;

import reactor.core.publisher.Mono;

@Service
public class EventPublisher {

        private static final Logger LOGGER = LoggerFactory.getLogger(EventPublisher.class);

        private final RabbitTemplate rabbitTemplate;
        private final ParkingEventSink parkingEventSink;

        public EventPublisher(RabbitTemplate rabbitTemplate, ParkingEventSink parkingEventSink) {
                this.rabbitTemplate = rabbitTemplate;
                this.parkingEventSink = parkingEventSink;
        }

        public Mono<Void> publishPaymentCompleted(
                        PaymentEventPayload payload,
                        long version) {

                Event event = Event.of(
                                RabbitMQConfig.PAYMENT_COMPLETED_KEY,
                                payload.paymentId().toString(),
                                version,
                                payload);

                return publish(
                                RabbitMQConfig.PARKING_EVENTS_EXCHANGE,
                                RabbitMQConfig.PAYMENT_COMPLETED_KEY,
                                event);
        }

        public Mono<Void> publishPaymentFailed(
                        PaymentEventPayload payload,
                        long version) {

                Event event = Event.of(
                                RabbitMQConfig.PAYMENT_FAILED_KEY,
                                payload.paymentId().toString(),
                                version,
                                payload);

                return publish(
                                RabbitMQConfig.PARKING_EVENTS_EXCHANGE,
                                RabbitMQConfig.PAYMENT_FAILED_KEY,
                                event);
        }

        public Mono<Void> publishVehicleEntry(
                        ParkingLifecycleEventPayload payload,
                        long version) {

                Event event = Event.of(
                                RabbitMQConfig.VEHICLE_ENTRY_KEY,
                                payload.session().sessionId().toString(),
                                version,
                                payload);

                parkingEventSink.emit(event);

                return publish(
                                RabbitMQConfig.PARKING_EVENTS_EXCHANGE,
                                RabbitMQConfig.VEHICLE_ENTRY_KEY,
                                event);
        }

        public Mono<Void> publishVehicleExit(
                        ParkingLifecycleEventPayload payload,
                        long version) {

                Event event = Event.of(
                                RabbitMQConfig.VEHICLE_EXIT_KEY,
                                payload.session().sessionId().toString(),
                                version,
                                payload);

                parkingEventSink.emit(event);

                return publish(
                                RabbitMQConfig.PARKING_EVENTS_EXCHANGE,
                                RabbitMQConfig.VEHICLE_EXIT_KEY,
                                event);
        }

        public Mono<Void> publishBillingRequested(
                        com.example.Metropark.event.payload.BillingRequestedEventPayload payload) {

                Event event = Event.of(
                                RabbitMQConfig.BILLING_REQUESTED_KEY,
                                payload.eventId(),
                                1,
                                payload);

                return publish(
                                RabbitMQConfig.PARKING_EVENTS_EXCHANGE,
                                RabbitMQConfig.BILLING_REQUESTED_KEY,
                                event);
        }

        public Mono<Void> publishNotificationEvent(
                        com.example.Metropark.event.payload.NotificationEventPayload payload) {

                Event event = Event.of(
                                RabbitMQConfig.NOTIFICATION_EVENT_KEY,
                                payload.eventId(),
                                1,
                                payload);

                return publish(
                                RabbitMQConfig.PARKING_EVENTS_EXCHANGE,
                                RabbitMQConfig.NOTIFICATION_EVENT_KEY,
                                event);
        }


        private Mono<Void> publish(
                        String exchange,
                        String routingKey,
                        Event event) {

                return Mono.fromRunnable(() -> {
                        rabbitTemplate.convertAndSend(
                                        exchange,
                                        routingKey,
                                        event);

                        LOGGER.debug(
                                        "Published event: {} to exchange: {} with routing key: {}",
                                        event.type(),
                                        exchange,
                                        routingKey);
                }).then();
        }
}

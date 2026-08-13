package com.example.Metropark.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.example.Metropark.event.ParkingEventSink;

@Configuration
public class RabbitMQConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConfig.class);

    public static final String PARKING_EVENTS_EXCHANGE = "parking.events";

    public static final String CAMERA_EVENTS_EXCHANGE = "parking-camera-events";

    public static final String LIFECYCLE_EVENTS_QUEUE = "parking.lifecycle.events";

    public static final String PAYMENT_EVENTS_QUEUE = "parking.payment.events";

    public static final String CAMERA_PROCESSING_QUEUE = "parking-camera-processing";

    public static final String CAMERA_AUDIT_QUEUE = "parking-camera-audit";

    public static final String LIFECYCLE_EVENTS_DLQ = "parking.lifecycle.events.dlq";

    public static final String PAYMENT_EVENTS_DLQ = "parking.payment.events.dlq";

    public static final String CAMERA_PROCESSING_DLQ = "parking-camera-processing.dlq";

    public static final String CAMERA_AUDIT_DLQ = "parking-camera-audit.dlq";

    public static final String CAMERA_CAR_ENTERED_KEY = "camera.car.entered";

    public static final String CAMERA_CAR_EXITED_KEY = "camera.car.exited";

    public static final String VEHICLE_ENTRY_KEY = "vehicle.entry";

    public static final String VEHICLE_EXIT_KEY = "vehicle.exit";

    public static final String PAYMENT_COMPLETED_KEY = "payment.completed";

    public static final String PAYMENT_FAILED_KEY = "payment.failed";

    public static final String BILLING_EVENTS_QUEUE = "parking.billing.events";

    public static final String BILLING_EVENTS_DLQ = "parking.billing.events.dlq";

    public static final String BILLING_REQUESTED_KEY = "billing.requested";

    public static final String NOTIFICATION_EVENTS_QUEUE = "parking.notification.events";

    public static final String NOTIFICATION_EVENTS_DLQ = "parking.notification.events.dlq";

    public static final String NOTIFICATION_EVENT_KEY = "notification.event";

    @Bean
    public MessageConverter messageConverter(
            ObjectMapper objectMapper) {

        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);

        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();

        typeMapper.addTrustedPackages(
                "com.example.Metropark");

        converter.setJavaTypeMapper(typeMapper);

        return converter;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter) {

        RabbitTemplate template = new RabbitTemplate(connectionFactory);

        template.setMessageConverter(messageConverter);
        template.setMandatory(true);

        template.setConfirmCallback(
                (correlation, ack, cause) -> {

                    String id = correlation != null
                            ? correlation.getId()
                            : "n/a";

                    if (ack) {
                        LOGGER.debug(
                                "RabbitMQ broker confirmed publish, correlationId={}",
                                id);
                    } else {
                        LOGGER.error(
                                "RabbitMQ broker NACKed publish, correlationId={}, cause={}",
                                id,
                                cause);
                    }
                });

        template.setReturnsCallback(
                returned -> LOGGER.error(
                        "RabbitMQ returned unroutable message: exchange={}, routingKey={}, replyCode={}, replyText={}",
                        returned.getExchange(),
                        returned.getRoutingKey(),
                        returned.getReplyCode(),
                        returned.getReplyText()));

        return template;
    }

    @Bean
    public ParkingEventSink parkingEventSink() {
        return new ParkingEventSink();
    }

    @Bean
    public DirectExchange parkingEventsExchange() {
        return new DirectExchange(
                PARKING_EVENTS_EXCHANGE,
                true,
                false);
    }

    @Bean
    public TopicExchange cameraEventsExchange() {
        return new TopicExchange(
                CAMERA_EVENTS_EXCHANGE,
                true,
                false);
    }

    @Bean
    public Queue lifecycleEventsDLQ() {
        return QueueBuilder
                .durable(LIFECYCLE_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue paymentEventsDLQ() {
        return QueueBuilder
                .durable(PAYMENT_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue cameraProcessingDLQ() {
        return QueueBuilder
                .durable(CAMERA_PROCESSING_DLQ)
                .build();
    }

    @Bean
    public Queue cameraAuditDLQ() {
        return QueueBuilder
                .durable(CAMERA_AUDIT_DLQ)
                .build();
    }

    @Bean
    public Queue lifecycleEventsQueue() {
        return QueueBuilder
                .durable(LIFECYCLE_EVENTS_QUEUE)
                .withArgument(
                        "x-dead-letter-exchange",
                        "")
                .withArgument(
                        "x-dead-letter-routing-key",
                        LIFECYCLE_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue paymentEventsQueue() {
        return QueueBuilder
                .durable(PAYMENT_EVENTS_QUEUE)
                .withArgument(
                        "x-dead-letter-exchange",
                        "")
                .withArgument(
                        "x-dead-letter-routing-key",
                        PAYMENT_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue cameraProcessingQueue() {
        return QueueBuilder
                .durable(CAMERA_PROCESSING_QUEUE)
                .withArgument(
                        "x-dead-letter-exchange",
                        "")
                .withArgument(
                        "x-dead-letter-routing-key",
                        CAMERA_PROCESSING_DLQ)
                .build();
    }

    @Bean
    public Queue cameraAuditQueue() {
        return QueueBuilder
                .durable(CAMERA_AUDIT_QUEUE)
                .withArgument(
                        "x-dead-letter-exchange",
                        "")
                .withArgument(
                        "x-dead-letter-routing-key",
                        CAMERA_AUDIT_DLQ)
                .build();
    }

    @Bean
    public Binding vehicleEntryBinding() {
        return BindingBuilder
                .bind(lifecycleEventsQueue())
                .to(parkingEventsExchange())
                .with(VEHICLE_ENTRY_KEY);
    }

    @Bean
    public Binding vehicleExitBinding() {
        return BindingBuilder
                .bind(lifecycleEventsQueue())
                .to(parkingEventsExchange())
                .with(VEHICLE_EXIT_KEY);
    }

    @Bean
    public Binding paymentCompletedBinding() {
        return BindingBuilder
                .bind(paymentEventsQueue())
                .to(parkingEventsExchange())
                .with(PAYMENT_COMPLETED_KEY);
    }

    @Bean
    public Binding paymentFailedBinding() {
        return BindingBuilder
                .bind(paymentEventsQueue())
                .to(parkingEventsExchange())
                .with(PAYMENT_FAILED_KEY);
    }

    @Bean
    public Queue billingEventsDLQ() {
        return QueueBuilder.durable(BILLING_EVENTS_DLQ).build();
    }

    @Bean
    public Queue notificationEventsDLQ() {
        return QueueBuilder.durable(NOTIFICATION_EVENTS_DLQ).build();
    }

    @Bean
    public Queue billingEventsQueue() {
        return QueueBuilder.durable(BILLING_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", BILLING_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue notificationEventsQueue() {
        return QueueBuilder.durable(NOTIFICATION_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", NOTIFICATION_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Binding billingRequestedBinding() {
        return BindingBuilder.bind(billingEventsQueue())
                .to(parkingEventsExchange())
                .with(BILLING_REQUESTED_KEY);
    }

    @Bean
    public Binding notificationEventBinding() {
        return BindingBuilder.bind(notificationEventsQueue())
                .to(parkingEventsExchange())
                .with(NOTIFICATION_EVENT_KEY);
    }


    private static final String CAMERA_ALL_KEYS = "camera.car.*";

    @Bean
    public Binding cameraProcessingBinding() {
        return BindingBuilder
                .bind(cameraProcessingQueue())
                .to(cameraEventsExchange())
                .with(CAMERA_ALL_KEYS);
    }

    @Bean
    public Binding cameraAuditBinding() {
        return BindingBuilder
                .bind(cameraAuditQueue())
                .to(cameraEventsExchange())
                .with(CAMERA_ALL_KEYS);
    }

    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();

        configurer.configure(
                factory,
                connectionFactory);

        factory.setMessageConverter(
                new SimpleMessageConverter());

        factory.setAcknowledgeMode(
                AcknowledgeMode.MANUAL);

        factory.setDefaultRequeueRejected(false);

        return factory;
    }
}

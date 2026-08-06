package com.example.Metropark.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    // Exchange names
    public static final String PARKING_EVENTS_EXCHANGE = "parking.events";
    public static final String CAMERA_EVENTS_EXCHANGE = "parking.camera.events";

    // Queue names
    public static final String SLOT_EVENTS_QUEUE = "parking.slot.events";
    public static final String RESERVATION_EVENTS_QUEUE = "parking.reservation.events";
    public static final String SESSION_EVENTS_QUEUE = "parking.session.events";
    public static final String PAYMENT_EVENTS_QUEUE = "parking.payment.events";
    public static final String CAMERA_EVENTS_QUEUE = "parking.camera.events.queue";
    
    // Dead Letter Queue names
    public static final String SLOT_EVENTS_DLQ = "parking.slot.events.dlq";
    public static final String RESERVATION_EVENTS_DLQ = "parking.reservation.events.dlq";
    public static final String SESSION_EVENTS_DLQ = "parking.session.events.dlq";
    public static final String PAYMENT_EVENTS_DLQ = "parking.payment.events.dlq";
    public static final String CAMERA_EVENTS_DLQ = "parking.camera.events.dlq";

    // Routing keys
    public static final String SLOT_CREATED_KEY = "slot.created";
    public static final String SLOT_UPDATED_KEY = "slot.updated";
    public static final String SLOT_DELETED_KEY = "slot.deleted";
    public static final String RESERVATION_CREATED_KEY = "reservation.created";
    public static final String RESERVATION_CANCELLED_KEY = "reservation.cancelled";
    public static final String SESSION_STARTED_KEY = "session.started";
    public static final String SESSION_ENDED_KEY = "session.ended";
    public static final String SESSION_STATUS_CHANGED_KEY = "session.status.changed";
    public static final String PAYMENT_COMPLETED_KEY = "payment.completed";
    public static final String PAYMENT_FAILED_KEY = "payment.failed";
    public static final String CAMERA_CAR_ENTERED_KEY = "camera.car.entered";
    public static final String CAMERA_CAR_EXITED_KEY = "camera.car.exited";

    @Bean
    public MessageConverter messageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter());
        template.setMandatory(true);
        return template;
    }

    // Exchanges
    @Bean
    public DirectExchange parkingEventsExchange() {
        return new DirectExchange(PARKING_EVENTS_EXCHANGE, true, false);
    }

    @Bean
    public DirectExchange cameraEventsExchange() {
        return new DirectExchange(CAMERA_EVENTS_EXCHANGE, true, false);
    }

    // Dead Letter Queues
    @Bean
    public Queue slotEventsDLQ() {
        return QueueBuilder.durable(SLOT_EVENTS_DLQ).build();
    }

    @Bean
    public Queue reservationEventsDLQ() {
        return QueueBuilder.durable(RESERVATION_EVENTS_DLQ).build();
    }

    @Bean
    public Queue sessionEventsDLQ() {
        return QueueBuilder.durable(SESSION_EVENTS_DLQ).build();
    }

    @Bean
    public Queue paymentEventsDLQ() {
        return QueueBuilder.durable(PAYMENT_EVENTS_DLQ).build();
    }

    @Bean
    public Queue cameraEventsDLQ() {
        return QueueBuilder.durable(CAMERA_EVENTS_DLQ).build();
    }

    // Main Queues with DLQ configuration
    @Bean
    public Queue slotEventsQueue() {
        return QueueBuilder.durable(SLOT_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", SLOT_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue reservationEventsQueue() {
        return QueueBuilder.durable(RESERVATION_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", RESERVATION_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue sessionEventsQueue() {
        return QueueBuilder.durable(SESSION_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", SESSION_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue paymentEventsQueue() {
        return QueueBuilder.durable(PAYMENT_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", PAYMENT_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Queue cameraEventsQueue() {
        return QueueBuilder.durable(CAMERA_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", CAMERA_EVENTS_DLQ)
                .build();
    }

    // Bindings for parking events
    @Bean
    public Binding slotCreatedBinding() {
        return BindingBuilder.bind(slotEventsQueue()).to(parkingEventsExchange()).with(SLOT_CREATED_KEY);
    }

    @Bean
    public Binding slotUpdatedBinding() {
        return BindingBuilder.bind(slotEventsQueue()).to(parkingEventsExchange()).with(SLOT_UPDATED_KEY);
    }

    @Bean
    public Binding slotDeletedBinding() {
        return BindingBuilder.bind(slotEventsQueue()).to(parkingEventsExchange()).with(SLOT_DELETED_KEY);
    }

    @Bean
    public Binding reservationCreatedBinding() {
        return BindingBuilder.bind(reservationEventsQueue()).to(parkingEventsExchange()).with(RESERVATION_CREATED_KEY);
    }

    @Bean
    public Binding reservationCancelledBinding() {
        return BindingBuilder.bind(reservationEventsQueue()).to(parkingEventsExchange()).with(RESERVATION_CANCELLED_KEY);
    }

    @Bean
    public Binding sessionStartedBinding() {
        return BindingBuilder.bind(sessionEventsQueue()).to(parkingEventsExchange()).with(SESSION_STARTED_KEY);
    }

    @Bean
    public Binding sessionEndedBinding() {
        return BindingBuilder.bind(sessionEventsQueue()).to(parkingEventsExchange()).with(SESSION_ENDED_KEY);
    }

    @Bean
    public Binding sessionStatusChangedBinding() {
        return BindingBuilder.bind(sessionEventsQueue()).to(parkingEventsExchange()).with(SESSION_STATUS_CHANGED_KEY);
    }

    @Bean
    public Binding paymentCompletedBinding() {
        return BindingBuilder.bind(paymentEventsQueue()).to(parkingEventsExchange()).with(PAYMENT_COMPLETED_KEY);
    }

    @Bean
    public Binding paymentFailedBinding() {
        return BindingBuilder.bind(paymentEventsQueue()).to(parkingEventsExchange()).with(PAYMENT_FAILED_KEY);
    }

    // Camera events bindings
    @Bean
    public Binding cameraCarEnteredBinding() {
        return BindingBuilder.bind(cameraEventsQueue()).to(cameraEventsExchange()).with(CAMERA_CAR_ENTERED_KEY);
    }

    @Bean
    public Binding cameraCarExitedBinding() {
        return BindingBuilder.bind(cameraEventsQueue()).to(cameraEventsExchange()).with(CAMERA_CAR_EXITED_KEY);
    }

    // Listener container factory for manual acknowledgment
    @Bean
    public SimpleMessageListenerContainer messageListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        container.setDefaultRequeueRejected(false);
        return container;
    }
}
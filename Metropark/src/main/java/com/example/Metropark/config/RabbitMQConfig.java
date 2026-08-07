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
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.support.converter.DefaultJackson2JavaTypeMapper;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.boot.amqp.autoconfigure.SimpleRabbitListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
public class RabbitMQConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQConfig.class);

    // Exchange names
    public static final String PARKING_EVENTS_EXCHANGE = "parking.events";

    /**
     * ANPR camera observations. A TOPIC exchange with one queue per concern, not a
     * direct exchange with one shared queue: two consumers on ONE queue compete and
     * each event reaches only one of them, so audit and parking would each see a
     * random half. Two queues bound to the same exchange each get their own copy.
     */
    public static final String CAMERA_EVENTS_EXCHANGE = "parking-camera-events";

    // Queue names
    public static final String SLOT_EVENTS_QUEUE = "parking.slot.events";
    public static final String RESERVATION_EVENTS_QUEUE = "parking.reservation.events";
    public static final String SESSION_EVENTS_QUEUE = "parking.session.events";
    public static final String PAYMENT_EVENTS_QUEUE = "parking.payment.events";
    public static final String LIFECYCLE_EVENTS_QUEUE = "parking.lifecycle.events";

    /** Drives parking: lock, allocate, park or exit. */
    public static final String CAMERA_PROCESSING_QUEUE = "parking-camera-processing";
    /** Maintains camera_events.status. Carries no parking logic. */
    public static final String CAMERA_AUDIT_QUEUE = "parking-camera-audit";

    // Dead Letter Queue names
    public static final String SLOT_EVENTS_DLQ = "parking.slot.events.dlq";
    public static final String RESERVATION_EVENTS_DLQ = "parking.reservation.events.dlq";
    public static final String SESSION_EVENTS_DLQ = "parking.session.events.dlq";
    public static final String PAYMENT_EVENTS_DLQ = "parking.payment.events.dlq";
    public static final String LIFECYCLE_EVENTS_DLQ = "parking.lifecycle.events.dlq";
    public static final String CAMERA_PROCESSING_DLQ = "parking-camera-processing.dlq";
    public static final String CAMERA_AUDIT_DLQ = "parking-camera-audit.dlq";

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

    // A vehicle entry / exit is one atomic PostgreSQL transaction spanning
    // parking_sessions, parking_slots and payments, so it gets its own queue
    // rather than being split across the three per-entity queues.
    public static final String VEHICLE_ENTRY_KEY = "vehicle.entry";
    public static final String VEHICLE_EXIT_KEY = "vehicle.exit";

    /**
     * Must reuse the application ObjectMapper (RedisConfig#objectMapper), which has
     * JavaTimeModule registered. The no-arg Jackson2JsonMessageConverter builds its
     * own bare ObjectMapper, which serialises Instant/LocalDateTime as reflective
     * bean graphs that the consumer's JSR-310 aware mapper cannot read back.
     */
    @Bean
    public MessageConverter messageConverter(ObjectMapper objectMapper) {
        Jackson2JsonMessageConverter converter = new Jackson2JsonMessageConverter(objectMapper);

        // The converter reads the class to instantiate from the __TypeId__ header and
        // refuses anything outside its trusted packages (java.util, java.lang by
        // default). Without this, every consumed Event failed conversion with
        // "class com.example.Metropark.event.Event is not in the trusted packages"
        // before the listener method was ever entered.
        DefaultJackson2JavaTypeMapper typeMapper = new DefaultJackson2JavaTypeMapper();
        typeMapper.addTrustedPackages("com.example.Metropark");
        converter.setJavaTypeMapper(typeMapper);
        return converter;
    }

    @Bean
    public RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, MessageConverter messageConverter) {
        RabbitTemplate template = new RabbitTemplate(connectionFactory);
        template.setMessageConverter(messageConverter);
        template.setMandatory(true);

        // spring.rabbitmq.publisher-confirm-type=correlated / publisher-returns=true
        // only surface failures if the callbacks are actually registered.
        template.setConfirmCallback((correlation, ack, cause) -> {
            String id = correlation != null ? correlation.getId() : "n/a";
            if (ack) {
                LOGGER.debug("RabbitMQ broker confirmed publish, correlationId={}", id);
            } else {
                LOGGER.error("RabbitMQ broker NACKed publish, correlationId={}, cause={}", id, cause);
            }
        });
        template.setReturnsCallback(returned -> LOGGER.error(
                "RabbitMQ returned unroutable message: exchange={}, routingKey={}, replyCode={}, replyText={}",
                returned.getExchange(),
                returned.getRoutingKey(),
                returned.getReplyCode(),
                returned.getReplyText()));

        return template;
    }

    // Exchanges
    @Bean
    public DirectExchange parkingEventsExchange() {
        return new DirectExchange(PARKING_EVENTS_EXCHANGE, true, false);
    }

    @Bean
    public TopicExchange cameraEventsExchange() {
        return new TopicExchange(CAMERA_EVENTS_EXCHANGE, true, false);
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
    public Queue cameraProcessingDLQ() {
        return QueueBuilder.durable(CAMERA_PROCESSING_DLQ).build();
    }

    @Bean
    public Queue cameraAuditDLQ() {
        return QueueBuilder.durable(CAMERA_AUDIT_DLQ).build();
    }

    @Bean
    public Queue lifecycleEventsDLQ() {
        return QueueBuilder.durable(LIFECYCLE_EVENTS_DLQ).build();
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
    public Queue cameraProcessingQueue() {
        return QueueBuilder.durable(CAMERA_PROCESSING_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", CAMERA_PROCESSING_DLQ)
                .build();
    }

    @Bean
    public Queue cameraAuditQueue() {
        return QueueBuilder.durable(CAMERA_AUDIT_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", CAMERA_AUDIT_DLQ)
                .build();
    }

    @Bean
    public Queue lifecycleEventsQueue() {
        return QueueBuilder.durable(LIFECYCLE_EVENTS_QUEUE)
                .withArgument("x-dead-letter-exchange", "")
                .withArgument("x-dead-letter-routing-key", LIFECYCLE_EVENTS_DLQ)
                .build();
    }

    @Bean
    public Binding vehicleEntryBinding() {
        return BindingBuilder.bind(lifecycleEventsQueue()).to(parkingEventsExchange()).with(VEHICLE_ENTRY_KEY);
    }

    @Bean
    public Binding vehicleExitBinding() {
        return BindingBuilder.bind(lifecycleEventsQueue()).to(parkingEventsExchange()).with(VEHICLE_EXIT_KEY);
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
        return BindingBuilder.bind(reservationEventsQueue()).to(parkingEventsExchange())
                .with(RESERVATION_CANCELLED_KEY);
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

    // Camera event bindings.
    //
    // Both queues bind the SAME wildcard, so every camera event is copied to each
    // of them. That is the whole point of the topic exchange: parking processing
    // and auditing are independent readers of one stream, not competitors for one
    // queue. Binding them to a single queue instead would hand roughly half the
    // events to each consumer.
    private static final String CAMERA_ALL_KEYS = "camera.car.*";

    @Bean
    public Binding cameraProcessingBinding() {
        return BindingBuilder.bind(cameraProcessingQueue()).to(cameraEventsExchange()).with(CAMERA_ALL_KEYS);
    }

    @Bean
    public Binding cameraAuditBinding() {
        return BindingBuilder.bind(cameraAuditQueue()).to(cameraEventsExchange()).with(CAMERA_ALL_KEYS);
    }

    /**
     * Built through the Boot configurer so that spring.rabbitmq.listener.simple.*
     * (acknowledge-mode, retry, requeue) is actually applied. A hand-rolled factory
     * silently ignores those properties and falls back to AUTO acknowledge, which
     * ACKs the message the moment the listener method returns - before any
     * asynchronous work it started has completed.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            SimpleRabbitListenerContainerFactoryConfigurer configurer,
            ConnectionFactory connectionFactory,
            MessageConverter messageConverter) {

        SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
        configurer.configure(factory, connectionFactory);

        // Deliberately NOT the Jackson converter: every listener takes the raw
        // org.springframework.amqp.core.Message and parses the envelope itself inside
        // a try/catch that dead letters on failure. A converter that can throw runs
        // *before* the listener method, and in MANUAL acknowledge mode a listener-side
        // exception is never nacked by the container - the delivery would stay
        // unacknowledged forever, invisible in both the DLQ and PostgreSQL.
        factory.setMessageConverter(new SimpleMessageConverter());

        // The consumer ACKs only after the PostgreSQL transaction has committed.
        factory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        factory.setDefaultRequeueRejected(false);
        return factory;
    }
}
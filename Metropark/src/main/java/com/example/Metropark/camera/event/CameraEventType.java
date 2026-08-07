package com.example.Metropark.camera.event;

/**
 * What the camera saw. The name doubles as the RabbitMQ routing key suffix
 * ({@code camera.car.entered} / {@code camera.car.exited}), so the two cannot
 * drift apart.
 */
public enum CameraEventType {

    CAR_ENTERED("camera.car.entered"),
    CAR_EXITED("camera.car.exited");

    private final String routingKey;

    CameraEventType(String routingKey) {
        this.routingKey = routingKey;
    }

    public String routingKey() {
        return routingKey;
    }
}

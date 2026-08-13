package com.example.Metropark.camera.event;

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

package com.example.Metropark.reservation.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReservationClassDto(
    @JsonProperty("classId") 
    Integer classId,
    
    @JsonProperty("className") 
    String className
) {}
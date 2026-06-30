package com.example.Metropark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record ReservationClassDto(
    @JsonProperty("classId") 
    Integer classId,
    
    @JsonProperty("className") 
    String className
) {}
package com.example.Metropark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record VehicleTypeDto(
    @JsonProperty("vehicleTypeId") 
    Integer vehicleTypeId,
    
    @JsonProperty("typeDisplayName") 
    String typeDisplayName
) {}
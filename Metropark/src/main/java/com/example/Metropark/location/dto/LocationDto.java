package com.example.Metropark.location.dto;

import com.fasterxml.jackson.annotation.JsonProperty;

public record LocationDto(
    @JsonProperty("locationId") 
    String locationId,
    
    @JsonProperty("typeId") 
    Integer typeId,
    
    @JsonProperty("locationName") 
    String locationName,
    
    @JsonProperty("city") 
    String city,
    
    @JsonProperty("status") 
    String status
) {}
package com.example.Metropark.location.dto;
import com.fasterxml.jackson.annotation.JsonProperty;

public record LocationTypeDto(
        Long typeId,
        @JsonProperty("typeName")
        String typeName) {
}
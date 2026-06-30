package com.example.Metropark.dto;
import com.fasterxml.jackson.annotation.JsonProperty;

public record LocationTypeDto(
        Long typeId,
        @JsonProperty("typeName")
        String typeName) {
}
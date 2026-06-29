package com.example.Metropark.dto;

public record LocationTypeDto(
    Integer typeId,
    String typeName,
    String address,
    String description
) {}
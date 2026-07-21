package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.math.BigDecimal;

public record RevenueDto(
    @JsonProperty("amount") BigDecimal amount,
    @JsonProperty("currency") String currency,
    @JsonProperty("transactionCount") long transactionCount,
    @JsonProperty("period") String period
) {}
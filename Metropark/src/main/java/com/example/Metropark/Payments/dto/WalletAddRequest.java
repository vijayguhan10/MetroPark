package com.example.Metropark.payments.dto;

import java.math.BigDecimal;

public record WalletAddRequest(
        String userId,
        BigDecimal amount
) {
}
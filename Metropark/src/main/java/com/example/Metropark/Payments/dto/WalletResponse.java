package com.example.Metropark.payments.dto;

import java.math.BigDecimal;

public record WalletResponse(
        String userId,
        BigDecimal amount,
        BigDecimal balance,
        String message
) {
}
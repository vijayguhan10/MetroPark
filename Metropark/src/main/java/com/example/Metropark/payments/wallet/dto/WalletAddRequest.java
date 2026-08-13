package com.example.Metropark.payments.wallet.dto;

import java.math.BigDecimal;

public record WalletAddRequest(
                String userId,
                BigDecimal amount) {
}
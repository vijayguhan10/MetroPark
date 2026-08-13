package com.example.Metropark.payments.service;

import java.math.BigDecimal;

import org.springframework.stereotype.Service;

import com.example.Metropark.payments.dto.WalletResponse;
import com.example.Metropark.payments.repo.WalletRepository;

import reactor.core.publisher.Mono;

@Service
public class WalletService {

    private final WalletRepository walletRepository;

    public WalletService(WalletRepository walletRepository) {
        this.walletRepository = walletRepository;
    }

    public Mono<WalletResponse> addFund(String userId, BigDecimal amount) {

        validate(userId, amount);

        return walletRepository.addFund(userId, amount)
                .flatMap(rows -> walletRepository.getFund(userId)
                        .map(balance -> new WalletResponse(
                                userId,
                                amount,
                                balance,
                                "Wallet fund added successfully"
                        )));
    }

    public Mono<WalletResponse> deductFund(String userId, BigDecimal amount) {

        validate(userId, amount);

        return walletRepository.deductFund(userId, amount)
                .flatMap(rows -> {

                    if (rows == 0) {
                        return Mono.error(
                                new IllegalStateException(
                                        "Insufficient wallet balance or wallet not found."
                                )
                        );
                    }

                    return walletRepository.getFund(userId)
                            .map(balance -> new WalletResponse(
                                    userId,
                                    amount,
                                    balance,
                                    "Wallet fund deducted successfully"
                            ));
                });
    }

    public Mono<WalletResponse> getBalance(String userId) {

        if (userId == null || userId.isBlank()) {
            return Mono.error(
                    new IllegalArgumentException("User ID is required.")
            );
        }

        return walletRepository.getFund(userId)
                .map(balance -> new WalletResponse(
                        userId,
                        BigDecimal.ZERO,
                        balance,
                        "Wallet balance fetched successfully"
                ));
    }

    private void validate(String userId, BigDecimal amount) {

        if (userId == null || userId.isBlank()) {
            throw new IllegalArgumentException("User ID is required.");
        }

        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException(
                    "Amount must be greater than zero."
            );
        }
    }
}
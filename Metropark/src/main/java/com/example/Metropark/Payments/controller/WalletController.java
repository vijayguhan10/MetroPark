package com.example.Metropark.payments.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.payments.dto.WalletAddRequest;
import com.example.Metropark.payments.dto.WalletResponse;
import com.example.Metropark.payments.service.WalletService;

import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/wallet")
public class WalletController {

    private final WalletService walletService;

    public WalletController(WalletService walletService) {
        this.walletService = walletService;
    }

    @PostMapping("/add")
    public Mono<ResponseEntity<WalletResponse>> addFund(
            @RequestBody WalletAddRequest request) {

        return walletService.addFund(
                        request.userId(),
                        request.amount()
                )
                .map(response ->
                        ResponseEntity.ok(response)
                );
    }


    @GetMapping("/{userId}")
    public Mono<ResponseEntity<WalletResponse>> getBalance(
            @PathVariable String userId) {

        return walletService.getBalance(userId)
                .map(response ->
                        ResponseEntity.ok(response)
                );
    }
}
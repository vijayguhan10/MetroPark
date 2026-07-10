package com.example.Metropark.payments.service;

import org.springframework.stereotype.Service;

import com.example.Metropark.payments.dto.PaymentHistoryDto;
import com.example.Metropark.payments.repo.PaymentHistoryRepository;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
public class PaymentHistoryService {

    private final PaymentHistoryRepository repository;

    public PaymentHistoryService(PaymentHistoryRepository repository) {
        this.repository = repository;
    }

    public Mono<Integer> createHistory(PaymentHistoryDto dto) {
        return repository.create(dto);
    }

    public Flux<PaymentHistoryDto> getAllHistory() {
        return repository.findAll();
    }

    public Mono<PaymentHistoryDto> getHistoryById(Long id) {
        return repository.findById(id);
    }

    public Flux<PaymentHistoryDto> getHistoryByPaymentId(Long paymentId) {
        return repository.findByPaymentId(paymentId);
    }
}
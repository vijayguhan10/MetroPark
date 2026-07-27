package com.example.Metropark.BFF.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.example.Metropark.BFF.dto.UserBusinessProfileDto;
import com.example.Metropark.BFF.service.UserParkingFrequencyService;

import reactor.core.publisher.Flux;

@RestController
@RequestMapping("/api/admin/user-parking-frequency")
public class UserParkingFrequencyController {

    private final UserParkingFrequencyService userParkingFrequencyService;

    public UserParkingFrequencyController(UserParkingFrequencyService userParkingFrequencyService) {
        this.userParkingFrequencyService = userParkingFrequencyService;
    }

    @GetMapping
    public Flux<UserBusinessProfileDto> getUltraRichUserInsights() {
        return userParkingFrequencyService.getUltraRichUserInsights();
    }
}
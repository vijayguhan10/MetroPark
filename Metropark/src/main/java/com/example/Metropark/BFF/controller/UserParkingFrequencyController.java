package com.example.Metropark.BFF.controller;

import com.example.Metropark.BFF.dto.UserParkingFrequencyDto;
import com.example.Metropark.BFF.service.UserParkingFrequencyService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/admin/user-parking-frequency")
public class UserParkingFrequencyController {

    private final UserParkingFrequencyService userParkingFrequencyService;

    public UserParkingFrequencyController(UserParkingFrequencyService userParkingFrequencyService) {
        this.userParkingFrequencyService = userParkingFrequencyService;
    }

    /**
     * Get combined user parking frequency data with sessions and all users detail
     * GET /api/admin/user-parking-frequency
     * 
     * Sample Response:
     * {
     *   "sessions": [
     *     {
     *       "userId": "USR-001",
     *       "name": "Rajesh Kumar",
     *       "email": "rajesh.kumar@email.com",
     *       "phone": "+91-9876543210",
     *       "totalSessions": 2,
     *       "totalDurationMinutes": 525,
     *       "totalSpent": 437.50,
     *       "lastParked": "2024-01-15T18:15:00"
     *     },
     *     ...
     *   ],
     *   "allUsersDetail": [
     *     {
     *       "userId": "USR-001",
     *       "name": "Rajesh Kumar",
     *       "email": "rajesh.kumar@email.com",
     *       "phone": "+91-9876543210",
     *       "joinedDate": "2023-05-10T00:00:00"
     *     },
     *     ...
     *   ]
     * }
     */
    @GetMapping
    public Mono<ResponseEntity<UserParkingFrequencyDto.UserParkingFrequencyResponse>> getUserParkingFrequency() {
        return userParkingFrequencyService.getUserParkingFrequency()
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * Get only user parking sessions (frequency data)
     * GET /api/admin/user-parking-frequency/sessions
     * 
     * Sample Response:
     * [
     *   {
     *     "userId": "USR-001",
     *     "name": "Rajesh Kumar",
     *     "email": "rajesh.kumar@email.com",
     *     "phone": "+91-9876543210",
     *     "totalSessions": 2,
     *     "totalDurationMinutes": 525,
     *     "totalSpent": 437.50,
     *     "lastParked": "2024-01-15T18:15:00"
     *   },
     *   ...
     * ]
     */
    @GetMapping("/sessions")
    public Mono<ResponseEntity<java.util.List<UserParkingFrequencyDto.UserParkingFrequencySummaryDto>>> getUserParkingSessions() {
        return userParkingFrequencyService.getUserParkingSessions()
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }

    /**
     * Get all users detail (joined date, etc.)
     * GET /api/admin/user-parking-frequency/users
     * 
     * Sample Response:
     * [
     *   {
     *     "userId": "USR-001",
     *     "name": "Rajesh Kumar",
     *     "email": "rajesh.kumar@email.com",
     *     "phone": "+91-9876543210",
     *     "joinedDate": "2023-05-10T00:00:00"
     *   },
     *   ...
     * ]
     */
    @GetMapping("/users")
    public Mono<ResponseEntity<java.util.List<UserParkingFrequencyDto.AllUsersDetailDto>>> getAllUsersDetail() {
        return userParkingFrequencyService.getAllUsersDetail()
                .map(ResponseEntity::ok)
                .defaultIfEmpty(ResponseEntity.notFound().build());
    }
}
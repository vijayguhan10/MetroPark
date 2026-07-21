package com.example.Metropark.BFF.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;
import java.util.List;

public record AdminDashboardDto(
    
    @JsonProperty("summary")
    DashboardSummaryDto summary,
    
    @JsonProperty("activeSessions")
    List<ActiveSessionDto> activeSessions,
    
    @JsonProperty("occupancyByLocation")
    List<OccupancyByLocationDto> occupancyByLocation,
    
    @JsonProperty("alerts")
    List<AlertDto> alerts,
    
    @JsonProperty("revenueToday")
    RevenueDto revenueToday,
    
    @JsonProperty("gateStatuses")
    List<GateStatusDto> gateStatuses,
    
    @JsonProperty("lastUpdated")
    LocalDateTime lastUpdated
) {}

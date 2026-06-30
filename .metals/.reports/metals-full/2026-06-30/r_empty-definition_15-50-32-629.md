error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/dto/ParkingSlotDto.java:java/time/LocalDateTime#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/dto/ParkingSlotDto.java
empty definition using pc, found symbol in pc: java/time/LocalDateTime#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 564
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/dto/ParkingSlotDto.java
text:
```scala
package com.example.Metropark.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.LocalDateTime;

public record ParkingSlotDto(
    @JsonProperty("slotId") Integer slotId,
    @JsonProperty("locationId") String locationId,
    @JsonProperty("displayCode") String displayCode,
    @JsonProperty("vehicleTypeId") Integer vehicleTypeId,
    @JsonProperty("reservationClassId") Integer reservationClassId,
    @JsonProperty("sensorId") String sensorId,
    @JsonProperty("currentStatus") String currentStatus,
    @JsonProperty("updatedAt") Lo@@calDateTime updatedAt
) {}
```


#### Short summary: 

empty definition using pc, found symbol in pc: java/time/LocalDateTime#
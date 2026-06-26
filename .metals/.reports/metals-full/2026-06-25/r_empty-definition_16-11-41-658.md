error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/dto/SuspensionDto.java:_empty_/LocalDateTime#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/dto/SuspensionDto.java
empty definition using pc, found symbol in pc: _empty_/LocalDateTime#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 197
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/dto/SuspensionDto.java
text:
```scala
package com.example.Metropark.dto;

package com.parking.dto;

import java.time.LocalDateTime;

public record SuspensionDto(
    Integer suspensionId,
    String userId,
    String reason,
    Local@@DateTime suspendedAt,
    LocalDateTime suspendedUntil,
    Boolean isActive,
    String authorizedBy
) {}
```


#### Short summary: 

empty definition using pc, found symbol in pc: _empty_/LocalDateTime#
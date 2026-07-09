error id: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/Payments/dto/PaymentDto.java:jakarta/validation/constraints/NotNull#
file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/Payments/dto/PaymentDto.java
empty definition using pc, found symbol in pc: jakarta/validation/constraints/NotNull#
empty definition using semanticdb
empty definition using fallback
non-local guesses:

offset: 137
uri: file:///C:/Users/vguhankm/projects/MetroPark/Metropark/src/main/java/com/example/Metropark/Payments/dto/PaymentDto.java
text:
```scala
package com.example.Metropark.payments.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.@@NotNull;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;

import java.math.BigDecimal;
import java.time.LocalDateTime;

public record PaymentDto(
        @JsonProperty("paymentId") Long paymentId,
        @JsonProperty("transactionReference") String transactionReference,
        @JsonProperty("reservationId") Integer reservationId,
        @JsonProperty("sessionId") Integer sessionId,
        @JsonProperty("methodId") @NotNull Integer methodId,
        @JsonProperty("amount") @NotNull @Positive BigDecimal amount,
        @JsonProperty("currency") @NotNull @Size(min = 3, max = 3) String currency,
        @JsonProperty("paymentStatus") String paymentStatus,
        @JsonProperty("gatewayResponseCode") String gatewayResponseCode,
        @JsonProperty("gatewayResponseMessage") String gatewayResponseMessage,
        @JsonProperty("processedAt") LocalDateTime processedAt,
        @JsonProperty("createdAt") LocalDateTime createdAt,
        @JsonProperty("updatedAt") LocalDateTime updatedAt) {
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: jakarta/validation/constraints/NotNull#
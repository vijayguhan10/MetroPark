package com.example.Metropark.payments.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.payments.dto.PaymentDto;

@Service
public class PaymentAuditLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentAuditLogger.class);

    public void logPaymentCreated(PaymentDto payment, Integer rowsAffected) {
        LOGGER.info(
                "Payment created: paymentId={}, transactionReference={}, sessionId={}, methodId={}, amount={}, currency={}, status={}, rowsAffected={}",
                payment.paymentId(),
                payment.transactionReference(),
                payment.sessionId(),
                payment.methodId(),
                payment.amount(),
                payment.currency(),
                payment.paymentStatus(),
                rowsAffected);
    }

    public void logPaymentStatusUpdated(
            Long paymentId,
            String previousStatus,
            String currentStatus,
            String changedBy,
            String reason,
            String gatewayReference) {

        LOGGER.info(
                "Payment status updated: paymentId={}, previousStatus={}, currentStatus={}, changedBy={}, reason={}, gatewayReference={}",
                paymentId,
                previousStatus,
                currentStatus,
                changedBy,
                reason,
                gatewayReference);
    }
}

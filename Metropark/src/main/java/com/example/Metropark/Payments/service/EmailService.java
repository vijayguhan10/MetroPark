package com.example.Metropark.payments.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.example.Metropark.event.payload.NotificationEventPayload;

import reactor.core.publisher.Mono;

@Service
public class EmailService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailService.class);

    public Mono<Void> sendNotificationEmail(NotificationEventPayload notif, String userEmail) {
        return Mono.fromRunnable(() -> {
            if ("SUCCESS".equalsIgnoreCase(notif.status())) {
                sendSuccessEmail(notif, userEmail);
            } else {
                sendFailureEmail(notif, userEmail);
            }
        });
    }

    private void sendSuccessEmail(NotificationEventPayload notif, String userEmail) {
        String subject = "MetroPark Parking Payment Successful";
        String body = String.format("""
            ==================================================
            Subject: %s
            To: %s
            --------------------------------------------------
            Dear Customer,

            Your parking session payment has been processed successfully.

            Session ID : %d
            Vehicle ID : %d
            Duration   : %d mins
            Amount     : ₹%.2f
            Status     : SUCCESS
            --------------------------------------------------
            Thank you for choosing MetroPark!
            ==================================================
            """, subject, userEmail != null ? userEmail : notif.userId(),
            notif.sessionId(), notif.vehicleId(), notif.durationMinutes(),
            notif.amount(), notif.status());

        LOGGER.info("\n--- EMAIL SENT (SMTP SIMULATION) ---\n{}", body);
    }

    private void sendFailureEmail(NotificationEventPayload notif, String userEmail) {
        String subject = "MetroPark Payment Failed — Account Suspended";
        String body = String.format("""
            ==================================================
            Subject: %s
            To: %s
            --------------------------------------------------
            Dear Customer,

            Your parking session ended but payment failed due to insufficient funds.
            Your account has been suspended.

            Session ID     : %d
            Vehicle ID     : %d
            Amount Due     : ₹%.2f
            Wallet Balance : ₹%.2f
            Status         : FAILED
            User Status    : SUSPENDED

            Message:
            Your parking session ended but payment failed due to insufficient funds. Your account has been suspended.
            --------------------------------------------------
            Please recharge your wallet to reactivate your account.
            ==================================================
            """, subject, userEmail != null ? userEmail : notif.userId(),
            notif.sessionId(), notif.vehicleId(), notif.amount(),
            notif.walletBalance() != null ? notif.walletBalance() : 0.00);

        LOGGER.warn("\n--- EMAIL SENT (SMTP SIMULATION) ---\n{}", body);
    }
}

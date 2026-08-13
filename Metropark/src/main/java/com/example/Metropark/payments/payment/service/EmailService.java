package com.example.Metropark.payments.payment.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Service;

import com.example.Metropark.event.payload.NotificationEventPayload;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

@Service
public class EmailService {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailService.class);

    private final JavaMailSender mailSender;
    private final String fromAddress;
    private final String toAddress;

    public EmailService(
            JavaMailSender mailSender,
            @Value("${metropark.mail.from}") String fromAddress,
            @Value("${metropark.mail.to}") String toAddress) {
        this.mailSender = mailSender;
        this.fromAddress = fromAddress;
        this.toAddress = toAddress;
    }

    public Mono<Void> sendNotificationEmail(NotificationEventPayload notif, String userEmail) {
        // Recipient is statically configured as vijayguhan10@gmail.com regardless of
        // the
        // user attached to the notification.
        String recipient = toAddress;
        return Mono.fromRunnable(() -> {
            if ("SUCCESS".equalsIgnoreCase(notif.status())) {
                sendSuccessEmail(notif, recipient);
            } else {
                sendFailureEmail(notif, recipient);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private void sendSuccessEmail(NotificationEventPayload notif, String recipient) {
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
                Amount     : Rs.%.2f
                Status     : SUCCESS
                --------------------------------------------------
                Thank you for choosing MetroPark!
                ==================================================
                """, subject, recipient,
                notif.sessionId(), notif.vehicleId(), notif.durationMinutes(),
                notif.amount(), notif.status());

        sendMail(recipient, subject, body);
        LOGGER.info("\n--- EMAIL SENT ---\n{}", body);
    }

    private void sendFailureEmail(NotificationEventPayload notif, String recipient) {
        String subject = "MetroPark Payment Failed - Account Suspended";
        String body = String.format(
                """
                        ==================================================
                        Subject: %s
                        To: %s
                        --------------------------------------------------
                        Dear Customer,

                        Your parking session ended but payment failed due to insufficient funds.
                        Your account has been suspended.

                        Session ID     : %d
                        Vehicle ID     : %d
                        Amount Due     : Rs.%.2f
                        Wallet Balance : Rs.%.2f
                        Status         : FAILED
                        User Status    : SUSPENDED

                        Message:
                        Your parking session ended but payment failed due to insufficient funds. Your account has been suspended.
                        --------------------------------------------------
                        Please recharge your wallet to reactivate your account.
                        ==================================================
                        """,
                subject, recipient,
                notif.sessionId(), notif.vehicleId(), notif.amount(),
                notif.walletBalance() != null ? notif.walletBalance() : 0.00);

        sendMail(recipient, subject, body);
        LOGGER.warn("\n--- EMAIL SENT ---\n{}", body);
    }

    private void sendMail(String recipient, String subject, String body) {
        try {
            SimpleMailMessage message = new SimpleMailMessage();
            message.setFrom(fromAddress);
            message.setTo(recipient);
            message.setSubject(subject);
            message.setText(body);
            mailSender.send(message);
        } catch (Exception e) {
            LOGGER.error("Failed to send email to {} (subject={}): {}", recipient, subject, e.getMessage());
        }
    }
}

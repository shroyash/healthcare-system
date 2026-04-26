//package com.healthcare.payment.config;
//
//import lombok.Data;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.stereotype.Component;
//
//@Data
//@Component
//@ConfigurationProperties(prefix = "refund")
//public class RefundProperties {
//
//    /**
//     * Cron expression for how often the RefundJob runs.
//     * Default: every 5 minutes
//     *
//     * Format: second minute hour day month weekday
//     * "0 *\/5 * * * *" = at second 0, every 5 minutes
//     *
//     * application.properties → refund.auto-process-cron
//     */
//    private String autoProcessCron;
//
//    /**
//     * How many times the RefundJob will retry a failed refund
//     * before giving up and flagging it for manual intervention.
//     *
//     * application.properties → refund.max-retry-attempts
//     */
//    private int maxRetryAttempts;
//
//    /**
//     * How many minutes to wait before retrying a failed refund.
//     * Each failed attempt schedules the next retry this many minutes later.
//     *
//     * application.properties → refund.retry-delay-minutes
//     */
//    private int retryDelayMinutes;
//}
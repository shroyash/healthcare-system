//package com.healthcare.payment.config;
//
//import lombok.Data;
//import org.springframework.boot.context.properties.ConfigurationProperties;
//import org.springframework.stereotype.Component;
//
//@Data
//@Component
//@ConfigurationProperties(prefix = "esewa")
//public class EsewaProperties {
//
//    /** eSewa merchant/product code. Sandbox: EPAYTEST */
//    private String productCode;
//
//    /** HMAC-SHA256 secret key for signing requests. Sandbox: 8gBm/:&EnhH.1/q */
//    private String secretKey;
//
//    /** eSewa payment form POST URL */
//    private String paymentUrl;
//
//    /** eSewa transaction status verification URL */
//    private String verifyUrl;
//
//    /** Your callback URL eSewa redirects to on success */
//    private String successUrl;
//
//    /** Your callback URL eSewa redirects to on failure/cancel */
//    private String failureUrl;
//
//    /** true = sandbox mode (simulates refunds, skips server verify) */
//    private boolean sandbox;
//}
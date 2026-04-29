package com.example.flink.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

/**
 * Represents a customer transaction event consumed from the Kafka topic.
 *
 * <p>Sample JSON payload:
 * <pre>
 * {
 *   "event_id":           "evt-1001",
 *   "customer_id":        "CUST123",
 *   "account_id":         "ACC999",
 *   "transaction_id":     "TXN001",
 *   "amount":             500.0,
 *   "currency":           "GBP",
 *   "transaction_status": "APPROVED",
 *   "account_status":     "ACTIVE",
 *   "transaction_time":   "2026-04-29T10:15:30Z"
 * }
 * </pre>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TransactionEvent implements Serializable {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("customer_id")
    private String customerId;

    @JsonProperty("account_id")
    private String accountId;

    @JsonProperty("transaction_id")
    private String transactionId;

    @JsonProperty("amount")
    private double amount;

    @JsonProperty("currency")
    private String currency;

    @JsonProperty("transaction_status")
    private String transactionStatus;

    @JsonProperty("account_status")
    private String accountStatus;

    @JsonProperty("transaction_time")
    private String transactionTime;

    // ---------------------- constructors ----------------------

    public TransactionEvent() {
    }

    // ---------------------- getters / setters ----------------------

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getTransactionStatus() {
        return transactionStatus;
    }

    public void setTransactionStatus(String transactionStatus) {
        this.transactionStatus = transactionStatus;
    }

    public String getAccountStatus() {
        return accountStatus;
    }

    public void setAccountStatus(String accountStatus) {
        this.accountStatus = accountStatus;
    }

    public String getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(String transactionTime) {
        this.transactionTime = transactionTime;
    }

    @Override
    public String toString() {
        return "TransactionEvent{" +
                "eventId='" + eventId + '\'' +
                ", customerId='" + customerId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", amount=" + amount +
                ", currency='" + currency + '\'' +
                ", transactionStatus='" + transactionStatus + '\'' +
                ", accountStatus='" + accountStatus + '\'' +
                '}';
    }
}

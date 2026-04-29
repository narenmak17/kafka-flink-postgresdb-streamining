package com.example.flink.model;

import java.io.Serializable;
import java.time.Instant;

/**
 * Represents the loan eligibility decision that is written to PostgreSQL.
 *
 * <p>This record is produced by {@link com.example.flink.function.EligibilityMapFunction}
 * after the eligibility rule has been evaluated against the incoming
 * {@link TransactionEvent}.
 *
 * <p>PostgreSQL target table: {@code loan_eligibility}
 * (see {@code docs/postgres-schema.sql}).
 */
public class LoanEligibilityResult implements Serializable {

    /** Possible values for {@link #eligibilityStatus}. */
    public static final String ELIGIBLE     = "ELIGIBLE";
    public static final String NOT_ELIGIBLE = "NOT_ELIGIBLE";

    private String customerId;
    private String accountId;
    private String transactionId;
    private double transactionAmountGbp;
    private String transactionCurrency;
    private String eligibilityStatus;
    private String eligibilityReason;
    private String loanRuleVersion;
    private String transactionTime;
    private Instant processedAt;

    // ---------------------- constructors ----------------------

    public LoanEligibilityResult() {
    }

    // ---------------------- getters / setters ----------------------

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

    public double getTransactionAmountGbp() {
        return transactionAmountGbp;
    }

    public void setTransactionAmountGbp(double transactionAmountGbp) {
        this.transactionAmountGbp = transactionAmountGbp;
    }

    public String getTransactionCurrency() {
        return transactionCurrency;
    }

    public void setTransactionCurrency(String transactionCurrency) {
        this.transactionCurrency = transactionCurrency;
    }

    public String getEligibilityStatus() {
        return eligibilityStatus;
    }

    public void setEligibilityStatus(String eligibilityStatus) {
        this.eligibilityStatus = eligibilityStatus;
    }

    public String getEligibilityReason() {
        return eligibilityReason;
    }

    public void setEligibilityReason(String eligibilityReason) {
        this.eligibilityReason = eligibilityReason;
    }

    public String getLoanRuleVersion() {
        return loanRuleVersion;
    }

    public void setLoanRuleVersion(String loanRuleVersion) {
        this.loanRuleVersion = loanRuleVersion;
    }

    public String getTransactionTime() {
        return transactionTime;
    }

    public void setTransactionTime(String transactionTime) {
        this.transactionTime = transactionTime;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    public void setProcessedAt(Instant processedAt) {
        this.processedAt = processedAt;
    }

    @Override
    public String toString() {
        return "LoanEligibilityResult{" +
                "customerId='" + customerId + '\'' +
                ", transactionId='" + transactionId + '\'' +
                ", eligibilityStatus='" + eligibilityStatus + '\'' +
                ", eligibilityReason='" + eligibilityReason + '\'' +
                '}';
    }
}

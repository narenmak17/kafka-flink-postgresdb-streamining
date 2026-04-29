package com.example.flink.model;

import java.io.Serializable;

public class LoanRuleConfig implements Serializable {

    private String ruleVersion;
    private double thresholdAmount;
    private String currency;
    private String requiredTransactionStatus;
    private String requiredAccountStatus;

    public LoanRuleConfig() {
    }

    public LoanRuleConfig(String ruleVersion, double thresholdAmount, String currency,
                          String requiredTransactionStatus, String requiredAccountStatus) {
        this.ruleVersion = ruleVersion;
        this.thresholdAmount = thresholdAmount;
        this.currency = currency;
        this.requiredTransactionStatus = requiredTransactionStatus;
        this.requiredAccountStatus = requiredAccountStatus;
    }

    public String getRuleVersion() {
        return ruleVersion;
    }

    public void setRuleVersion(String ruleVersion) {
        this.ruleVersion = ruleVersion;
    }

    public double getThresholdAmount() {
        return thresholdAmount;
    }

    public void setThresholdAmount(double thresholdAmount) {
        this.thresholdAmount = thresholdAmount;
    }

    public String getCurrency() {
        return currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public String getRequiredTransactionStatus() {
        return requiredTransactionStatus;
    }

    public void setRequiredTransactionStatus(String requiredTransactionStatus) {
        this.requiredTransactionStatus = requiredTransactionStatus;
    }

    public String getRequiredAccountStatus() {
        return requiredAccountStatus;
    }

    public void setRequiredAccountStatus(String requiredAccountStatus) {
        this.requiredAccountStatus = requiredAccountStatus;
    }
}
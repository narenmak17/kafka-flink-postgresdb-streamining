package com.example.flink.rule;

import com.example.flink.config.AppConfig;
import com.example.flink.model.LoanEligibilityResult;
import com.example.flink.model.TransactionEvent;

import java.io.Serializable;

/**
 * Evaluates whether a {@link TransactionEvent} meets the loan eligibility criteria.
 *
 * <p>Default rule (all conditions must be true):
 * <ul>
 *   <li>amount &gt;= configured threshold (default 500)</li>
 *   <li>currency == configured currency (default GBP)</li>
 *   <li>transaction_status == configured status (default APPROVED)</li>
 *   <li>account_status == configured status (default ACTIVE)</li>
 * </ul>
 *
 * <p>All thresholds / required values come from {@link AppConfig} so they can be
 * changed via environment variables without touching the code.
 */
public class LoanEligibilityRule implements Serializable {

    private final double thresholdAmount;
    private final String currency;
    private final String requiredTransactionStatus;
    private final String requiredAccountStatus;
    private final String ruleVersion;

    public LoanEligibilityRule(AppConfig config) {
        this.thresholdAmount             = config.getRuleThresholdAmount();
        this.currency                    = config.getRuleCurrency();
        this.requiredTransactionStatus   = config.getRuleRequiredTransactionStatus();
        this.requiredAccountStatus       = config.getRuleRequiredAccountStatus();
        this.ruleVersion                 = config.getLoanRuleVersion();
    }

    /**
     * Evaluates the rule and returns a populated {@link LoanEligibilityResult}.
     *
     * @param event the transaction event from Kafka
     * @return eligibility result ready to be written to PostgreSQL
     */
    public LoanEligibilityResult evaluate(TransactionEvent event) {
        LoanEligibilityResult result = new LoanEligibilityResult();
        result.setCustomerId(event.getCustomerId());
        result.setAccountId(event.getAccountId());
        result.setTransactionId(event.getTransactionId());
        result.setTransactionAmountGbp(event.getAmount());
        result.setTransactionCurrency(event.getCurrency());
        result.setLoanRuleVersion(ruleVersion);
        result.setTransactionTime(event.getTransactionTime());
        result.setProcessedAt(java.time.Instant.now());

        if (isEligible(event)) {
            result.setEligibilityStatus(LoanEligibilityResult.ELIGIBLE);
            result.setEligibilityReason(
                    "Meets threshold (" + thresholdAmount + " " + currency +
                    ") and account/transaction status rules");
        } else {
            result.setEligibilityStatus(LoanEligibilityResult.NOT_ELIGIBLE);
            result.setEligibilityReason(buildNotEligibleReason(event));
        }

        return result;
    }

    // ----------------------------------------------------------------
    //  Helpers
    // ----------------------------------------------------------------

    private boolean isEligible(TransactionEvent event) {
        return event.getAmount() >= thresholdAmount
                && currency.equalsIgnoreCase(event.getCurrency())
                && requiredTransactionStatus.equalsIgnoreCase(event.getTransactionStatus())
                && requiredAccountStatus.equalsIgnoreCase(event.getAccountStatus());
    }

    private String buildNotEligibleReason(TransactionEvent event) {
        StringBuilder sb = new StringBuilder("Not eligible:");
        if (event.getAmount() < thresholdAmount) {
            sb.append(" amount ").append(event.getAmount())
              .append(" < threshold ").append(thresholdAmount).append(";");
        }
        if (!currency.equalsIgnoreCase(event.getCurrency())) {
            sb.append(" currency ").append(event.getCurrency())
              .append(" != required ").append(currency).append(";");
        }
        if (!requiredTransactionStatus.equalsIgnoreCase(event.getTransactionStatus())) {
            sb.append(" transaction_status ").append(event.getTransactionStatus())
              .append(" != required ").append(requiredTransactionStatus).append(";");
        }
        if (!requiredAccountStatus.equalsIgnoreCase(event.getAccountStatus())) {
            sb.append(" account_status ").append(event.getAccountStatus())
              .append(" != required ").append(requiredAccountStatus).append(";");
        }
        return sb.toString();
    }
}

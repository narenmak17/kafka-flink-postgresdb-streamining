package com.example.flink.function;

import com.example.flink.config.AppConfig;
import com.example.flink.model.LoanEligibilityResult;
import com.example.flink.model.TransactionEvent;
import com.example.flink.rule.LoanEligibilityRule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Flink {@link MapFunction} that:
 * <ol>
 *   <li>Parses the raw JSON string from Kafka into a {@link TransactionEvent}</li>
 *   <li>Applies the {@link LoanEligibilityRule}</li>
 *   <li>Returns a {@link LoanEligibilityResult}</li>
 * </ol>
 *
 * <p>If parsing fails, the record is skipped and a warning is logged.
 * A dead-letter topic can be added here in a future iteration.
 */
public class EligibilityMapFunction implements MapFunction<String, LoanEligibilityResult> {

    private static final Logger LOG = LoggerFactory.getLogger(EligibilityMapFunction.class);

    private final AppConfig config;
    private transient ObjectMapper objectMapper;
    private transient LoanEligibilityRule rule;

    public EligibilityMapFunction(AppConfig config) {
        this.config = config;
    }

    @Override
    public LoanEligibilityResult map(String json) throws Exception {
        // Lazily initialise non-serialisable helpers on first use
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        if (rule == null) {
            rule = new LoanEligibilityRule(config);
        }

        try {
            TransactionEvent event = objectMapper.readValue(json, TransactionEvent.class);
            LoanEligibilityResult result = rule.evaluate(event);
            LOG.info("Processed event {} -> status={}", event.getTransactionId(), result.getEligibilityStatus());
            return result;
        } catch (Exception e) {
            LOG.warn("Failed to process record: {}. Error: {}", json, e.getMessage());
            // Return a sentinel result rather than crashing the job.
            // Extend this to write to a dead-letter topic if needed.
            LoanEligibilityResult errorResult = new LoanEligibilityResult();
            errorResult.setCustomerId("UNKNOWN");
            errorResult.setAccountId("UNKNOWN");
            errorResult.setTransactionId("UNKNOWN");
            errorResult.setTransactionAmountGbp(0.0);
            errorResult.setTransactionCurrency("UNKNOWN");
            errorResult.setEligibilityStatus(LoanEligibilityResult.NOT_ELIGIBLE);
            errorResult.setEligibilityReason("Parse error: " + e.getMessage());
            errorResult.setLoanRuleVersion(config.getLoanRuleVersion());
            errorResult.setTransactionTime("");
            errorResult.setProcessedAt(java.time.Instant.now());
            return errorResult;
        }
    }
}

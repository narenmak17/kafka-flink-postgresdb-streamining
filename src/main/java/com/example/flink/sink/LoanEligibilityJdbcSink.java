package com.example.flink.sink;

import com.example.flink.model.LoanEligibilityResult;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;
import java.time.Instant;

/**
 * Factory that creates a Flink {@link SinkFunction} which writes
 * {@link LoanEligibilityResult} records to the {@code loan_eligibility}
 * PostgreSQL table via the Flink JDBC connector.
 *
 * <p>Tune {@link JdbcExecutionOptions} to control batching behaviour:
 * <ul>
 *   <li>{@code batchSize} – records per batch insert (default 100)</li>
 *   <li>{@code batchIntervalMs} – max wait in ms before flushing (default 200 ms)</li>
 *   <li>{@code maxRetries} – retry count on transient DB errors (default 3)</li>
 * </ul>
 */
public class LoanEligibilityJdbcSink {

    private static final String INSERT_SQL =
            "INSERT INTO loan_eligibility " +
            "(customer_id, account_id, transaction_id, transaction_amount_gbp, " +
            " transaction_currency, eligibility_status, eligibility_reason, " +
            " loan_rule_version, transaction_time, processed_at) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?::timestamptz, ?)";

    private LoanEligibilityJdbcSink() {
        // Utility class – use the static factory method below.
    }

    /**
     * Creates and returns the JDBC sink.
     *
     * @param jdbcUrl        JDBC connection URL, e.g. {@code jdbc:postgresql://localhost:5432/loan_db}
     * @param username       database username
     * @param password       database password
     * @return configured Flink sink function
     */
    public static SinkFunction<LoanEligibilityResult> create(
            String jdbcUrl, String username, String password) {

        return JdbcSink.sink(
                INSERT_SQL,
                (ps, result) -> {
                    ps.setString(1, result.getCustomerId());
                    ps.setString(2, result.getAccountId());
                    ps.setString(3, result.getTransactionId());
                    ps.setDouble(4, result.getTransactionAmountGbp());
                    ps.setString(5, result.getTransactionCurrency());
                    ps.setString(6, result.getEligibilityStatus());
                    ps.setString(7, result.getEligibilityReason());
                    ps.setString(8, result.getLoanRuleVersion());
                    ps.setString(9, result.getTransactionTime());
                    Instant processedAt = result.getProcessedAt() != null
                            ? result.getProcessedAt() : Instant.now();
                    ps.setTimestamp(10, Timestamp.from(processedAt));
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(username)
                        .withPassword(password)
                        .build()
        );
    }
}

package com.example.flink;

import com.example.flink.config.AppConfig;
import com.example.flink.model.LoanEligibilityResult;
import com.example.flink.model.LoanRuleConfig;
import com.example.flink.model.TransactionEvent;
import com.example.flink.sink.LoanEligibilityJdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.Properties;

/**
 * Flink job that:
 * 1) reads customer transaction events from Kafka
 * 2) reads loan rule updates from a Kafka rules topic
 * 3) broadcasts the latest rules
 * 4) evaluates each transaction against the latest rule
 * 5) writes eligibility results to PostgreSQL
 */
public class LoanEligibilityJob {

    private static final Logger LOG = LoggerFactory.getLogger(LoanEligibilityJob.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());

    // Broadcast state descriptor for current rule
    private static final MapStateDescriptor<String, LoanRuleConfig> RULE_STATE_DESCRIPTOR =
            new MapStateDescriptor<>("loan-rules-state", String.class, LoanRuleConfig.class);

    public static void main(String[] args) throws Exception {
        AppConfig config = new AppConfig();

        LOG.info("Starting LoanEligibilityJob with config: transactionsTopic={}, rulesTopic={}, bootstrapServers={}",
                config.getKafkaTransactionsTopic(),
                config.getKafkaRulesTopic(),
                config.getKafkaBootstrapServers());

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties kafkaProps = buildKafkaProperties(config);

        KafkaSource<String> transactionSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaTransactionsTopic())
                .setGroupId(config.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();

        KafkaSource<String> rulesSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaRulesTopic())
                .setGroupId(config.getKafkaGroupId() + "-rules")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();

        DataStream<String> transactionStream = env.fromSource(
                transactionSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Transaction Source"
        );

        DataStream<String> ruleStream = env.fromSource(
                rulesSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Rules Source"
        );

        DataStream<TransactionEvent> transactionEvents = transactionStream
                .map(new TransactionEventParser())
                .name("Parse Transaction Event");

       DataStream<LoanRuleConfig> ruleConfigs = ruleStream
        .map(value -> {
            try {
                LoanRuleConfig parsed = OBJECT_MAPPER.readValue(value, LoanRuleConfig.class);

                if (parsed.getRuleVersion() == null || parsed.getRuleVersion().isBlank()) {
                    parsed.setRuleVersion(config.getLoanRuleVersion());
                }
                if (parsed.getCurrency() == null || parsed.getCurrency().isBlank()) {
                    parsed.setCurrency(config.getRuleCurrency());
                }
                if (parsed.getRequiredTransactionStatus() == null || parsed.getRequiredTransactionStatus().isBlank()) {
                    parsed.setRequiredTransactionStatus(config.getRuleRequiredTransactionStatus());
                }
                if (parsed.getRequiredAccountStatus() == null || parsed.getRequiredAccountStatus().isBlank()) {
                    parsed.setRequiredAccountStatus(config.getRuleRequiredAccountStatus());
                }
                if (parsed.getThresholdAmount() <= 0) {
                    parsed.setThresholdAmount(config.getRuleThresholdAmount());
                }

                return parsed;
            } catch (Exception e) {
                LOG.warn("Failed to parse rule event '{}', falling back to config defaults. Error: {}", value, e.getMessage());
                return new LoanRuleConfig(
                        config.getLoanRuleVersion(),
                        config.getRuleThresholdAmount(),
                        config.getRuleCurrency(),
                        config.getRuleRequiredTransactionStatus(),
                        config.getRuleRequiredAccountStatus()
                );
            }
        })
        .name("Parse Rule Config Event");

        BroadcastStream<LoanRuleConfig> broadcastRules = ruleConfigs.broadcast(RULE_STATE_DESCRIPTOR);

        DataStream<LoanEligibilityResult> eligibilityResults = transactionEvents
                .connect(broadcastRules)
                .process(new LoanEligibilityBroadcastProcessFunction())
                .name("Apply Loan Eligibility Rules");

        DataStreamSink<LoanEligibilityResult> sink = eligibilityResults.addSink(
                LoanEligibilityJdbcSink.create(
                        config.getPostgresUrl(),
                        config.getPostgresUsername(),
                        config.getPostgresPassword(),
                        config.getPostgresTableName()
                )
        ).name("PostgreSQL Loan Eligibility Sink");

        env.execute("Loan Eligibility Streaming Job");
    }

    private static Properties buildKafkaProperties(AppConfig config) {
        Properties props = new Properties();

        String securityProtocol = config.getKafkaSecurityProtocol();
        if (securityProtocol != null && !securityProtocol.isBlank()
                && !"PLAINTEXT".equalsIgnoreCase(securityProtocol)) {
            props.setProperty("security.protocol", securityProtocol);

            String saslMechanism = config.getKafkaSaslMechanism();
            if (saslMechanism != null && !saslMechanism.isBlank()) {
                props.setProperty("sasl.mechanism", saslMechanism);
            }

            String saslJaas = config.getKafkaSaslJaasConfig();
            if (saslJaas != null && !saslJaas.isBlank()) {
                props.setProperty("sasl.jaas.config", saslJaas);
            }
        }

        return props;
    }

    /**
     * Parses raw transaction JSON into TransactionEvent.
     */
    public static class TransactionEventParser implements MapFunction<String, TransactionEvent> {
        @Override
        public TransactionEvent map(String value) throws Exception {
            return JsonUtils.fromJson(value, TransactionEvent.class);
        }
    }

    /**
     * Parses raw rule JSON into LoanRuleConfig.
     * If the Kafka config topic is not used or message is malformed, this falls back
     * to the environment/default config values from AppConfig.
     */
    public static class LoanRuleConfigParser implements MapFunction<String, LoanRuleConfig> {

        private final AppConfig fallbackConfig;

        public LoanRuleConfigParser(AppConfig fallbackConfig) {
            this.fallbackConfig = fallbackConfig;
        }

        @Override
        public LoanRuleConfig map(String value) throws Exception {
            try {
                LoanRuleConfig parsed = JsonUtils.fromJson(value, LoanRuleConfig.class);

                // Basic fallback safety
                if (parsed.getRuleVersion() == null || parsed.getRuleVersion().isBlank()) {
                    parsed.setRuleVersion(fallbackConfig.getLoanRuleVersion());
                }
                if (parsed.getCurrency() == null || parsed.getCurrency().isBlank()) {
                    parsed.setCurrency(fallbackConfig.getRuleCurrency());
                }
                if (parsed.getRequiredTransactionStatus() == null || parsed.getRequiredTransactionStatus().isBlank()) {
                    parsed.setRequiredTransactionStatus(fallbackConfig.getRuleRequiredTransactionStatus());
                }
                if (parsed.getRequiredAccountStatus() == null || parsed.getRequiredAccountStatus().isBlank()) {
                    parsed.setRequiredAccountStatus(fallbackConfig.getRuleRequiredAccountStatus());
                }
                if (parsed.getThresholdAmount() <= 0) {
                    parsed.setThresholdAmount(fallbackConfig.getRuleThresholdAmount());
                }

                return parsed;
            } catch (Exception e) {
                LOG.warn("Failed to parse rule event '{}', falling back to config defaults. Error: {}", value, e.getMessage());
                return new LoanRuleConfig(
                        fallbackConfig.getLoanRuleVersion(),
                        fallbackConfig.getRuleThresholdAmount(),
                        fallbackConfig.getRuleCurrency(),
                        fallbackConfig.getRuleRequiredTransactionStatus(),
                        fallbackConfig.getRuleRequiredAccountStatus()
                );
            }
        }
    }

    /**
     * Broadcast process function that:
     * - updates the latest rule config from the rules topic
     * - evaluates each transaction against the latest rule
     */
    public static class LoanEligibilityBroadcastProcessFunction
            extends BroadcastProcessFunction<TransactionEvent, LoanRuleConfig, LoanEligibilityResult> {

        @Override
        public void processBroadcastElement(
                LoanRuleConfig rule,
                Context ctx,
                Collector<LoanEligibilityResult> out) throws Exception {

            ctx.getBroadcastState(RULE_STATE_DESCRIPTOR).put("current", rule);
            LOG.info("Updated broadcast rule config: version={}, threshold={}, currency={}",
                    rule.getRuleVersion(), rule.getThresholdAmount(), rule.getCurrency());
        }

        @Override
        public void processElement(
                TransactionEvent event,
                ReadOnlyContext ctx,
                Collector<LoanEligibilityResult> out) throws Exception {

            LoanRuleConfig rule = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR).get("current");

            if (rule == null) {
                LOG.warn("No rule config available yet. Skipping transactionId={}", event.getTransactionId());
                return;
            }

            boolean eligible =
                    event.getAmount() >= rule.getThresholdAmount()
                            && safeEquals(event.getCurrency(), rule.getCurrency())
                            && safeEquals(event.getTransactionStatus(), rule.getRequiredTransactionStatus())
                            && safeEquals(event.getAccountStatus(), rule.getRequiredAccountStatus());

            String status = eligible ? "ELIGIBLE" : "NOT_ELIGIBLE";
            String reason = eligible
                    ? "Meets rule conditions"
                    : "Does not meet threshold/currency/status conditions";

            LoanEligibilityResult result = new LoanEligibilityResult(
                    event.getCustomerId(),
                    event.getAccountId(),
                    event.getTransactionId(),
                    event.getAmount(),
                    event.getCurrency(),
                    status,
                    reason,
                    rule.getRuleVersion(),
                    event.getTransactionTime(),
                    Instant.now().toString()
            );

            out.collect(result);
        }

        private boolean safeEquals(String a, String b) {
            return a != null && b != null && a.equalsIgnoreCase(b);
        }
    }
}
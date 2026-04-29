package com.example.flink.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.io.Serializable;
import java.util.Map;

/**
 * Reads application configuration from environment variables, falling back to
 * values defined in {@code application.yml} on the classpath.
 *
 * <p><strong>Priority (highest first):</strong>
 * <ol>
 *   <li>Environment variable</li>
 *   <li>Value in {@code application.yml}</li>
 *   <li>Hard-coded default</li>
 * </ol>
 *
 * <p>This keeps the configuration simple and beginner-friendly while making
 * the application deployable to Confluent Cloud, AWS MSK, or a local setup
 * without code changes.
 */
public class AppConfig implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AppConfig.class);
    private static final String CONFIG_FILE = "application.yml";

    // -------------------- Kafka --------------------
    private final String kafkaBootstrapServers;
    private final String kafkaTransactionsTopic;
    private final String kafkaGroupId;
    private final String kafkaSecurityProtocol;
    private final String kafkaSaslMechanism;
    private final String kafkaSaslJaasConfig;

    // -------------------- PostgreSQL --------------------
    private final String postgresUrl;
    private final String postgresUsername;
    private final String postgresPassword;
    private final String postgresTableName;

    // -------------------- Loan rule --------------------
    private final double ruleThresholdAmount;
    private final String ruleCurrency;
    private final String ruleRequiredTransactionStatus;
    private final String ruleRequiredAccountStatus;
    private final String loanRuleVersion;

    @SuppressWarnings("unchecked")
    public AppConfig() {
        Map<String, Object> yaml = loadYaml();
        Map<String, Object> app  = getSection(yaml, "app");
        Map<String, Object> kafka = getSection(app, "kafka");
        Map<String, Object> pg   = getSection(app, "postgres");
        Map<String, Object> rules = getSection(app, "rules");

        // Kafka
        kafkaBootstrapServers   = resolve("KAFKA_BOOTSTRAP_SERVERS",   kafka, "bootstrap-servers",            "localhost:9092");
        kafkaTransactionsTopic  = resolve("KAFKA_TRANSACTIONS_TOPIC",  kafka, "transactions-topic",           "customer.transactions");
        kafkaGroupId            = resolve("KAFKA_GROUP_ID",            kafka, "group-id",                    "flink-loan-eligibility-group");
        kafkaSecurityProtocol   = resolve("KAFKA_SECURITY_PROTOCOL",   kafka, "security-protocol",           "PLAINTEXT");
        kafkaSaslMechanism      = resolve("KAFKA_SASL_MECHANISM",      kafka, "sasl-mechanism",              "");
        kafkaSaslJaasConfig     = resolve("KAFKA_SASL_JAAS_CONFIG",    kafka, "sasl-jaas-config",            "");

        // PostgreSQL
        postgresUrl      = resolve("POSTGRES_URL",      pg, "url",       "jdbc:postgresql://localhost:5432/loan_db");
        postgresUsername = resolve("POSTGRES_USERNAME", pg, "username",  "flink");
        postgresPassword = resolve("POSTGRES_PASSWORD", pg, "password",  "flink");
        postgresTableName = resolve("POSTGRES_TABLE",   pg, "table-name","loan_eligibility");

        // Rules
        String thresholdStr = resolve("RULE_THRESHOLD_AMOUNT", rules, "threshold-amount", "500.0");
        ruleThresholdAmount              = Double.parseDouble(thresholdStr);
        ruleCurrency                     = resolve("RULE_CURRENCY",                       rules, "currency",                       "GBP");
        ruleRequiredTransactionStatus    = resolve("RULE_REQUIRED_TRANSACTION_STATUS",    rules, "required-transaction-status",    "APPROVED");
        ruleRequiredAccountStatus        = resolve("RULE_REQUIRED_ACCOUNT_STATUS",        rules, "required-account-status",        "ACTIVE");
        loanRuleVersion                  = resolve("LOAN_RULE_VERSION",                   rules, "loan-rule-version",              "v1");

        LOG.info("AppConfig loaded: bootstrapServers={}, topic={}, postgresUrl={}",
                kafkaBootstrapServers, kafkaTransactionsTopic, postgresUrl);
    }

    // ----------------------------------------------------------------
    //  Internal helpers
    // ----------------------------------------------------------------

    @SuppressWarnings("unchecked")
    private Map<String, Object> loadYaml() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (is == null) {
                LOG.warn("{} not found on classpath; using env vars / defaults only.", CONFIG_FILE);
                return Map.of();
            }
            Yaml yaml = new Yaml();
            Map<String, Object> result = yaml.load(is);
            return result != null ? result : Map.of();
        } catch (Exception e) {
            LOG.warn("Could not load {}: {}. Falling back to env vars / defaults.", CONFIG_FILE, e.getMessage());
            return Map.of();
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getSection(Map<String, Object> parent, String key) {
        if (parent == null) return Map.of();
        Object value = parent.get(key);
        return (value instanceof Map) ? (Map<String, Object>) value : Map.of();
    }

    /**
     * Returns the first non-null, non-empty value from:
     * 1. environment variable {@code envKey}
     * 2. YAML key {@code yamlKey} inside {@code section}
     * 3. {@code defaultValue}
     */
    private static String resolve(String envKey, Map<String, Object> section,
                                  String yamlKey, String defaultValue) {
        String envValue = System.getenv(envKey);
        if (envValue != null && !envValue.isBlank()) {
            return envValue;
        }
        if (section != null) {
            Object yamlValue = section.get(yamlKey);
            if (yamlValue != null && !yamlValue.toString().isBlank()) {
                return yamlValue.toString();
            }
        }
        return defaultValue;
    }

    // ----------------------------------------------------------------
    //  Public accessors
    // ----------------------------------------------------------------

    public String getKafkaBootstrapServers()        { return kafkaBootstrapServers; }
    public String getKafkaTransactionsTopic()       { return kafkaTransactionsTopic; }
    public String getKafkaGroupId()                 { return kafkaGroupId; }
    public String getKafkaSecurityProtocol()        { return kafkaSecurityProtocol; }
    public String getKafkaSaslMechanism()           { return kafkaSaslMechanism; }
    public String getKafkaSaslJaasConfig()          { return kafkaSaslJaasConfig; }

    public String getPostgresUrl()                  { return postgresUrl; }
    public String getPostgresUsername()             { return postgresUsername; }
    public String getPostgresPassword()             { return postgresPassword; }
    public String getPostgresTableName()            { return postgresTableName; }

    public double getRuleThresholdAmount()          { return ruleThresholdAmount; }
    public String getRuleCurrency()                 { return ruleCurrency; }
    public String getRuleRequiredTransactionStatus(){ return ruleRequiredTransactionStatus; }
    public String getRuleRequiredAccountStatus()    { return ruleRequiredAccountStatus; }
    public String getLoanRuleVersion()              { return loanRuleVersion; }
}

package com.example.flink;

import com.example.flink.config.AppConfig;
import com.example.flink.function.EligibilityMapFunction;
import com.example.flink.model.LoanEligibilityResult;
import com.example.flink.sink.LoanEligibilityJdbcSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Entry point for the Loan Eligibility Flink streaming application.
 *
 * <p><strong>What this job does:</strong>
 * <ol>
 *   <li>Reads JSON transaction events from a Kafka topic</li>
 *   <li>Parses each event into a {@link com.example.flink.model.TransactionEvent}</li>
 *   <li>Evaluates the loan eligibility rule via {@link EligibilityMapFunction}</li>
 *   <li>Writes the {@link LoanEligibilityResult} to PostgreSQL via JDBC sink</li>
 * </ol>
 *
 * <p><strong>Running locally:</strong>
 * <pre>
 *   mvn clean package
 *   java -jar target/kafka-flink-postgresdb-streaming-1.0.0-SNAPSHOT.jar
 * </pre>
 *
 * <p><strong>Submitting to a Flink cluster:</strong>
 * <pre>
 *   flink run target/kafka-flink-postgresdb-streaming-1.0.0-SNAPSHOT.jar
 * </pre>
 *
 * <p>All configuration is read from environment variables and/or
 * {@code application.yml} on the classpath.  See {@link AppConfig} for details.
 */
public class LoanEligibilityJob {

    private static final Logger LOG = LoggerFactory.getLogger(LoanEligibilityJob.class);

    public static void main(String[] args) throws Exception {

        // 1. Load configuration
        AppConfig config = new AppConfig();
        LOG.info("Starting LoanEligibilityJob with config: bootstrapServers={}, topic={}",
                config.getKafkaBootstrapServers(), config.getKafkaTransactionsTopic());

        // 2. Set up Flink execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 3. Build Kafka consumer properties
        Properties kafkaProps = buildKafkaProperties(config);

        // 4. Create Kafka source
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(config.getKafkaBootstrapServers())
                .setTopics(config.getKafkaTransactionsTopic())
                .setGroupId(config.getKafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperties(kafkaProps)
                .build();

        // 5. Build the pipeline
        DataStream<String> rawEvents = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka Transaction Source"
        );

        DataStream<LoanEligibilityResult> eligibilityResults = rawEvents
                .map(new EligibilityMapFunction(config))
                .name("Loan Eligibility Rule Evaluation");

        eligibilityResults.addSink(
                LoanEligibilityJdbcSink.create(
                        config.getPostgresUrl(),
                        config.getPostgresUsername(),
                        config.getPostgresPassword()
                )
        ).name("PostgreSQL Loan Eligibility Sink");

        // 6. Execute
        env.execute("Loan Eligibility Streaming Job");
    }

    /**
     * Builds Kafka {@link Properties} for authentication.
     *
     * <p>The security settings are only added when a non-blank
     * security protocol is configured so the app works equally well
     * with a plain local Kafka broker.
     */
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
}

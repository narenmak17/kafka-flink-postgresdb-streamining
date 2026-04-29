# Local Setup Guide

Step-by-step instructions for running the Loan Eligibility Flink application locally.

---

## Prerequisites

| Tool            | Version | Install                                                                  |
|-----------------|---------|--------------------------------------------------------------------------|
| Java            | 17      | https://adoptium.net/ or `brew install temurin@17`                       |
| Maven           | 3.9+    | https://maven.apache.org/download.cgi or `brew install maven`            |
| Docker Desktop  | any     | https://www.docker.com/products/docker-desktop/                          |
| Kafka (source)  | —       | Confluent Cloud trial **or** local Kafka (see below)                     |

Optional:
- `psql` CLI for inspecting PostgreSQL results
- Confluent CLI for producing test events

---

## Step 1: Clone the repo

```bash
git clone https://github.com/narenmak17/kafka-flink-postgresdb-streamining.git
cd kafka-flink-postgresdb-streamining
```

---

## Step 2: Start PostgreSQL with Docker

```bash
docker compose up -d postgres
```

Verify it is running:

```bash
docker compose ps
# or
psql -h localhost -U flink -d loan_db -c "\dt"
```

The schema is applied automatically on first start (see `docs/postgres-schema.sql`).

---

## Step 3: Set environment variables

Create a file called `.env.local` (it is git-ignored) and source it:

```bash
# .env.local – never commit this file

# --- Kafka (Confluent Cloud example) ---
export KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_API_KEY" password="YOUR_API_SECRET";'
export KAFKA_TRANSACTIONS_TOPIC=customer.transactions

# --- PostgreSQL (local Docker) ---
export POSTGRES_URL=jdbc:postgresql://localhost:5432/loan_db
export POSTGRES_USERNAME=flink
export POSTGRES_PASSWORD=flink

# --- Loan rule ---
export RULE_THRESHOLD_AMOUNT=500.0
export RULE_CURRENCY=GBP
export RULE_REQUIRED_TRANSACTION_STATUS=APPROVED
export RULE_REQUIRED_ACCOUNT_STATUS=ACTIVE
export LOAN_RULE_VERSION=v1
```

Source the file:

```bash
source .env.local
```

> For a **plain local Kafka** (no auth), you only need:
> ```bash
> export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
> export KAFKA_TRANSACTIONS_TOPIC=customer.transactions
> ```

---

## Step 4: Build the fat JAR

```bash
mvn clean package -DskipTests
```

The output JAR is: `target/kafka-flink-postgresdb-streaming-1.0.0-SNAPSHOT.jar`

---

## Step 5: Create the Kafka topic

### Using Confluent Cloud UI
1. Log in → your cluster → Topics → Add topic
2. Name: `customer.transactions`
3. Partitions: 3

### Using Confluent CLI
```bash
confluent kafka topic create customer.transactions \
  --partitions 3 \
  --replication-factor 3
```

### Using Kafka CLI (local broker)
```bash
kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic customer.transactions \
  --partitions 3 --replication-factor 1
```

---

## Step 6: Run the Flink job

```bash
java -jar target/kafka-flink-postgresdb-streaming-1.0.0-SNAPSHOT.jar
```

You should see log output like:

```
INFO  LoanEligibilityJob - Starting LoanEligibilityJob with config: bootstrapServers=...
INFO  LoanEligibilityJob - Executing: Loan Eligibility Streaming Job
```

The job will start consuming from the latest offset and block until you stop it (`Ctrl+C`).

---

## Step 7: Produce sample events

Open a new terminal and produce one of the three sample events below.

### Event 1 – should produce ELIGIBLE

```bash
# Confluent CLI
confluent kafka topic produce customer.transactions \
  --value-format string

# Paste this and press Enter:
{"event_id":"evt-1001","customer_id":"CUST123","account_id":"ACC999","transaction_id":"TXN001","amount":500.0,"currency":"GBP","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:15:30Z"}
```

### Event 2 – should produce NOT_ELIGIBLE (amount too low)

```json
{"event_id":"evt-1002","customer_id":"CUST456","account_id":"ACC888","transaction_id":"TXN002","amount":250.0,"currency":"GBP","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:20:00Z"}
```

### Event 3 – should produce NOT_ELIGIBLE (wrong currency)

```json
{"event_id":"evt-1003","customer_id":"CUST789","account_id":"ACC777","transaction_id":"TXN003","amount":700.0,"currency":"USD","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:25:00Z"}
```

See `scripts/produce-sample-events.sh` for an automated script.

---

## Step 8: Verify results in PostgreSQL

```bash
psql -h localhost -U flink -d loan_db
```

```sql
SELECT customer_id, transaction_id, transaction_amount_gbp,
       transaction_currency, eligibility_status, eligibility_reason,
       loan_rule_version, processed_at
FROM loan_eligibility
ORDER BY processed_at DESC;
```

Expected output:

| customer_id | transaction_id | eligibility_status | eligibility_reason                                  |
|-------------|-----------------|---------------------|-----------------------------------------------------|
| CUST123     | TXN001          | ELIGIBLE            | Meets threshold (500.0 GBP) ...                     |
| CUST456     | TXN002          | NOT_ELIGIBLE        | Not eligible: amount 250.0 < threshold 500.0        |
| CUST789     | TXN003          | NOT_ELIGIBLE        | Not eligible: currency USD != required GBP          |

---

## Troubleshooting

### Flink job crashes immediately
- Check that environment variables are set: `echo $KAFKA_BOOTSTRAP_SERVERS`
- Check that PostgreSQL is running: `docker compose ps`

### Cannot connect to Kafka
- Verify your `KAFKA_BOOTSTRAP_SERVERS` value
- For Confluent Cloud, check that your API key has permission on the topic
- Try producing from the Confluent CLI to validate connectivity

### No data in PostgreSQL after producing events
- Check Flink log output for parse errors
- Verify the topic name matches `KAFKA_TRANSACTIONS_TOPIC`
- Make sure the JSON payload matches the expected schema (see `docs/architecture.md`)

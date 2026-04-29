# Confluent Cloud Setup Guide

This guide walks you through setting up **Confluent Cloud Kafka** as the source for the Loan Eligibility Flink application.

---

## Prerequisites

- A Confluent Cloud account (free trial is sufficient): https://confluent.cloud
- The Confluent CLI installed (optional but helpful): https://docs.confluent.io/confluent-cli/current/install.html

---

## Step 1: Create a Kafka cluster

1. Log in to https://confluent.cloud
2. Click **Add cluster** → **Basic** (free tier is fine)
3. Choose a cloud provider and region closest to you
4. Give it a name, e.g. `loan-demo-cluster`
5. Click **Launch cluster**

---

## Step 2: Create the Kafka topic

1. In your cluster, go to **Topics** → **Add topic**
2. Name: `customer.transactions`
3. Partitions: 3 (or 1 for the demo)
4. Click **Save & create**

---

## Step 3: Create an API key

1. Go to **Data integration** → **API keys** → **Add key**
2. Scope: **Cluster** (not global)
3. Click **Download and continue** – save the key and secret securely

---

## Step 4: Get the bootstrap server

1. Go to **Cluster settings** → **Endpoints**
2. Copy the **Bootstrap server** value, e.g.:
   ```
   pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
   ```

---

## Step 5: Set environment variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=PLAIN
export KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.plain.PlainLoginModule required username="YOUR_API_KEY" password="YOUR_API_SECRET";'
export KAFKA_TRANSACTIONS_TOPIC=customer.transactions
```

---

## Step 6: Produce test events

Use the Confluent Cloud UI:
1. Go to **Topics** → `customer.transactions` → **Messages**
2. Click **Produce a new message to this topic**
3. Paste the sample payload below

Or use the Confluent CLI:

```bash
confluent kafka topic produce customer.transactions \
  --value-format string \
  --bootstrap pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
```

Then paste:

```json
{"event_id":"evt-1001","customer_id":"CUST123","account_id":"ACC999","transaction_id":"TXN001","amount":500.0,"currency":"GBP","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:15:30Z"}
```

---

## Step 7: Run the Flink job

With the env vars set:

```bash
java -jar target/kafka-flink-postgresdb-streaming-1.0.0-SNAPSHOT.jar
```

---

## Notes on schema registry

This demo uses plain JSON (no schema registry).  
If you later want to add Confluent Schema Registry, you will need to:

1. Create a schema for `TransactionEvent` in the Confluent Cloud Schema Registry
2. Replace `SimpleStringSchema` with an Avro or JSON schema deserializer
3. Add the schema registry URL and API key to your config

---

## Confluent Cloud Flink (optional)

You can also run this job directly on **Confluent Cloud Flink** (managed Flink service):

1. Go to **Flink** → **Compute pools** → **Add compute pool**
2. Use **Flink SQL** or **custom JAR** (custom JAR is the DataStream path)
3. Follow: https://docs.confluent.io/cloud/current/flink/index.html

For the starter demo, running Flink locally against Confluent Cloud Kafka is simpler.

# AWS MSK Setup Guide

This guide shows which settings to change when using **AWS MSK** (Managed Streaming for Apache Kafka) as the Kafka source instead of Confluent Cloud.

The application code does **not** change — only environment variables.

---

## MSK auth options

| Auth method            | `KAFKA_SECURITY_PROTOCOL` | `KAFKA_SASL_MECHANISM` | Notes                                  |
|------------------------|---------------------------|------------------------|----------------------------------------|
| No auth (dev/test)     | `PLAINTEXT`               | *(blank)*              | Only inside a VPC, never public        |
| TLS only               | `SSL`                     | *(blank)*              | Uses AWS-managed cert                  |
| SASL/SCRAM             | `SASL_SSL`                | `SCRAM-SHA-512`        | Username/password stored in Secrets Mgr|
| IAM authentication     | `SASL_SSL`                | `AWS_MSK_IAM`          | Uses IAM roles/policies                |

---

## Option A: SASL/SCRAM (username + password)

### Step 1: Create SCRAM credentials in AWS Secrets Manager

Follow: https://docs.aws.amazon.com/msk/latest/developerguide/msk-password.html

### Step 2: Set environment variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=b-1.mycluster.xxx.c2.kafka.us-east-1.amazonaws.com:9096
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=SCRAM-SHA-512
export KAFKA_SASL_JAAS_CONFIG='org.apache.kafka.common.security.scram.ScramLoginModule required username="YOUR_USERNAME" password="YOUR_PASSWORD";'
export KAFKA_TRANSACTIONS_TOPIC=customer.transactions
```

> **Note:** MSK SASL/SCRAM uses port **9096**, not 9092.

---

## Option B: IAM authentication

### Step 1: Add the AWS MSK IAM library to pom.xml

The Flink job needs the AWS MSK IAM auth library on the classpath.

Add this dependency to `pom.xml`:

```xml
<dependency>
    <groupId>software.amazon.msk</groupId>
    <artifactId>aws-msk-iam-auth</artifactId>
    <version>2.1.1</version>
</dependency>
```

### Step 2: Attach an IAM policy to your execution role

Minimum IAM permissions needed:

```json
{
  "Effect": "Allow",
  "Action": [
    "kafka-cluster:Connect",
    "kafka-cluster:DescribeGroup",
    "kafka-cluster:AlterGroup",
    "kafka-cluster:DescribeTopic",
    "kafka-cluster:ReadData",
    "kafka-cluster:DescribeClusterDynamicConfiguration"
  ],
  "Resource": [
    "arn:aws:kafka:REGION:ACCOUNT:cluster/CLUSTER_NAME/CLUSTER_ID",
    "arn:aws:kafka:REGION:ACCOUNT:topic/CLUSTER_NAME/CLUSTER_ID/*",
    "arn:aws:kafka:REGION:ACCOUNT:group/CLUSTER_NAME/CLUSTER_ID/*"
  ]
}
```

### Step 3: Set environment variables

```bash
export KAFKA_BOOTSTRAP_SERVERS=b-1.mycluster.xxx.c2.kafka.us-east-1.amazonaws.com:9098
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=AWS_MSK_IAM
export KAFKA_SASL_JAAS_CONFIG='software.amazon.msk.auth.iam.IAMLoginModule required;'
export KAFKA_TRANSACTIONS_TOPIC=customer.transactions
```

> **Note:** MSK IAM auth uses port **9098**.

---

## Option C: TLS only (no SASL)

```bash
export KAFKA_BOOTSTRAP_SERVERS=b-1.mycluster.xxx.c2.kafka.us-east-1.amazonaws.com:9094
export KAFKA_SECURITY_PROTOCOL=SSL
export KAFKA_TRANSACTIONS_TOPIC=customer.transactions
```

If MSK uses a private CA, you may need to add a truststore:

```bash
export KAFKA_SASL_JAAS_CONFIG='ssl.truststore.location=/path/to/truststore.jks ssl.truststore.password=changeit;'
```

---

## MSK vs Confluent Cloud: what changes

| Setting                    | Confluent Cloud              | AWS MSK (SCRAM)                         |
|----------------------------|------------------------------|-----------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`  | `pkc-xxx.confluent.cloud:9092` | `b-1.cluster.kafka.region.amazonaws.com:9096` |
| `KAFKA_SECURITY_PROTOCOL`  | `SASL_SSL`                   | `SASL_SSL`                              |
| `KAFKA_SASL_MECHANISM`     | `PLAIN`                      | `SCRAM-SHA-512`                         |
| `KAFKA_SASL_JAAS_CONFIG`   | `PlainLoginModule ...`       | `ScramLoginModule ...`                  |
| Extra dependency           | None                         | `aws-msk-iam-auth` (IAM only)           |

Everything else (PostgreSQL, rules, Flink job code) is identical.

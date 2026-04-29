#!/usr/bin/env bash
# =============================================================
# Produce three sample transaction events to the Kafka topic.
# =============================================================
# Prerequisites:
#   - Confluent CLI installed and logged in
#   - KAFKA_BOOTSTRAP_SERVERS env var set
#   - KAFKA_TRANSACTIONS_TOPIC env var set (default: customer.transactions)
#
# Usage:
#   chmod +x scripts/produce-sample-events.sh
#   ./scripts/produce-sample-events.sh
# =============================================================

set -euo pipefail

TOPIC="${KAFKA_TRANSACTIONS_TOPIC:-customer.transactions}"
BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"

echo "Producing sample events to topic: $TOPIC (broker: $BOOTSTRAP)"

# Event 1: ELIGIBLE
EVENT_1='{"event_id":"evt-1001","customer_id":"CUST123","account_id":"ACC999","transaction_id":"TXN001","amount":500.0,"currency":"GBP","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:15:30Z"}'

# Event 2: NOT_ELIGIBLE (amount below threshold)
EVENT_2='{"event_id":"evt-1002","customer_id":"CUST456","account_id":"ACC888","transaction_id":"TXN002","amount":250.0,"currency":"GBP","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:20:00Z"}'

# Event 3: NOT_ELIGIBLE (wrong currency)
EVENT_3='{"event_id":"evt-1003","customer_id":"CUST789","account_id":"ACC777","transaction_id":"TXN003","amount":700.0,"currency":"USD","transaction_status":"APPROVED","account_status":"ACTIVE","transaction_time":"2026-04-29T10:25:00Z"}'

produce_event() {
    local event="$1"
    if command -v kafka-console-producer.sh &>/dev/null; then
        echo "$event" | kafka-console-producer.sh \
            --bootstrap-server "$BOOTSTRAP" \
            --topic "$TOPIC"
    elif command -v confluent &>/dev/null; then
        echo "$event" | confluent kafka topic produce "$TOPIC" \
            --value-format string
    else
        echo "WARNING: neither kafka-console-producer.sh nor confluent CLI found."
        echo "Paste this event manually into your Kafka producer:"
        echo "$event"
    fi
}

produce_event "$EVENT_1"
echo "Produced Event 1 (ELIGIBLE expected): TXN001"

produce_event "$EVENT_2"
echo "Produced Event 2 (NOT_ELIGIBLE expected): TXN002"

produce_event "$EVENT_3"
echo "Produced Event 3 (NOT_ELIGIBLE expected): TXN003"

echo ""
echo "Done. Check PostgreSQL for results:"
echo "  psql -h localhost -U flink -d loan_db -c 'SELECT customer_id, transaction_id, eligibility_status FROM loan_eligibility ORDER BY processed_at DESC LIMIT 5;'"

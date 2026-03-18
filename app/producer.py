#!/usr/bin/env python3
import json
import os
import time
from datetime import datetime, timezone

from confluent_kafka import Producer


def kafka_config() -> dict:
    cfg = {
        "bootstrap.servers": os.getenv("BROKER", "10.20.30.40:19092"),
        "client.id": "confluent-manual-producer",
        "enable.idempotence": True,
        "acks": "all",
        "retries": 1000000,
        "linger.ms": 10,
    }

    sec = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper()
    cfg["security.protocol"] = sec

    if sec in {"SSL", "SASL_SSL"}:
        cafile = os.getenv("SSL_CAFILE", "")
        if cafile:
            cfg["ssl.ca.location"] = cafile
        cfg["ssl.endpoint.identification.algorithm"] = (
            "https" if os.getenv("SSL_CHECK_HOSTNAME", "true").lower() == "true" else "none"
        )

    if sec in {"SASL_SSL", "SASL_PLAINTEXT"}:
        cfg["sasl.mechanism"] = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
        cfg["sasl.username"] = os.getenv("KAFKA_SASL_USERNAME", "")
        cfg["sasl.password"] = os.getenv("KAFKA_SASL_PASSWORD", "")

    return cfg


def on_delivery(err, msg):
    if err is not None:
        print(f"[producer] delivery failed: {err}")
    else:
        print(f"[producer] delivered topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")


def produce_batch(producer: Producer, topic: str, start_event_id: int, batch_size: int) -> int:
    sent = 0
    for i in range(batch_size):
        event_id = start_event_id + i
        payload = {
            "event_id": event_id,
            "source": "confluent-manual-commit",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        producer.produce(
            topic,
            key=str(event_id),
            value=json.dumps(payload).encode("utf-8"),
            callback=on_delivery,
        )
        producer.poll(0)
        sent += 1

    producer.flush(30)
    return sent


def main() -> int:
    topic = os.getenv("TOPIC", "transactions")
    run_mode = os.getenv("PRODUCER_RUN_MODE", "continuous").lower()
    interval = float(os.getenv("PRODUCER_INTERVAL_SEC", "30"))
    batch_size = int(os.getenv("PRODUCER_BATCH_SIZE", "100"))
    event_id = int(os.getenv("PRODUCER_START_EVENT_ID", "0"))

    producer = Producer(kafka_config())
    print(
        f"[producer] started topic={topic} mode={run_mode} interval={interval}s batch_size={batch_size}"
    )

    if run_mode not in {"continuous", "interval", "oneshot"}:
        raise ValueError(
            f"invalid PRODUCER_RUN_MODE={run_mode}. Use: continuous|interval|oneshot"
        )

    try:
        if run_mode == "continuous":
            while True:
                sent = produce_batch(producer, topic, event_id, 1)
                event_id += sent
                time.sleep(interval)

        elif run_mode == "interval":
            while True:
                sent = produce_batch(producer, topic, event_id, batch_size)
                event_id += sent
                time.sleep(interval)

        else:
            sent = produce_batch(producer, topic, event_id, batch_size)
            print(f"[producer] oneshot completed, sent={sent}")

    except KeyboardInterrupt:
        print("[producer] stopping")
    finally:
        producer.flush(30)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

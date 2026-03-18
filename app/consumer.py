#!/usr/bin/env python3
import json
import os
import time

from confluent_kafka import Consumer


def kafka_config() -> dict:
    cfg = {
        "bootstrap.servers": os.getenv("BROKER", "10.20.30.40:19092"),
        "group.id": os.getenv("GROUP_ID", "confluent-manual-group"),
        "auto.offset.reset": os.getenv("AUTO_OFFSET_RESET", "earliest"),
        "enable.auto.commit": False,
        "client.id": "confluent-manual-consumer",
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


def process_message(raw: bytes) -> dict:
    # Здесь может быть бизнес-логика записи в БД / валидации.
    return json.loads(raw.decode("utf-8"))


def consume_batch(consumer: Consumer, max_messages: int, batch_timeout_sec: float) -> int:
    processed = 0
    started_at = time.time()

    while processed < max_messages and (time.time() - started_at) < batch_timeout_sec:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"[consumer] error: {msg.error()}")
            continue

        try:
            payload = process_message(msg.value())
            print(
                f"[consumer] processed topic={msg.topic()} partition={msg.partition()} offset={msg.offset()} payload={payload}"
            )
            # Коммит только после успешной обработки.
            consumer.commit(message=msg, asynchronous=False)
            processed += 1
        except Exception as exc:
            print(f"[consumer] processing failed at offset={msg.offset()}: {exc}")
            # Без commit: сообщение будет перечитано (at-least-once).

    return processed


def main() -> int:
    topic = os.getenv("TOPIC", "transactions")
    run_mode = os.getenv("CONSUMER_RUN_MODE", "continuous").lower()
    interval_sec = float(os.getenv("CONSUMER_INTERVAL_SEC", "30"))
    batch_size = int(os.getenv("CONSUMER_BATCH_SIZE", "100"))
    batch_timeout_sec = float(os.getenv("CONSUMER_BATCH_TIMEOUT_SEC", "10"))

    consumer = Consumer(kafka_config())
    consumer.subscribe([topic])

    print(
        f"[consumer] started topic={topic} group_id={os.getenv('GROUP_ID')} mode={run_mode} "
        f"interval={interval_sec}s batch_size={batch_size} batch_timeout={batch_timeout_sec}s"
    )

    if run_mode not in {"continuous", "interval", "oneshot"}:
        raise ValueError(
            f"invalid CONSUMER_RUN_MODE={run_mode}. Use: continuous|interval|oneshot"
        )

    try:
        if run_mode == "continuous":
            while True:
                consume_batch(consumer, max_messages=1, batch_timeout_sec=1.0)
        elif run_mode == "interval":
            while True:
                processed = consume_batch(consumer, batch_size, batch_timeout_sec)
                print(f"[consumer] interval batch done processed={processed}")
                time.sleep(interval_sec)
        else:
            processed = consume_batch(consumer, batch_size, batch_timeout_sec)
            print(f"[consumer] oneshot completed processed={processed}")

    except KeyboardInterrupt:
        print("[consumer] stopping")
    finally:
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

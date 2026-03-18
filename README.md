# Docker Kafka Clients (Confluent Manual Commit)

Вариант 2: producer/consumer на `confluent-kafka` с более продовым паттерном.

## Запуск

```bash
cp .env.example .env
cp env/producer.env.example env/producer.env
cp env/consumer.env.example env/consumer.env
cp env/oracle-producer-archivelog.env.example env/oracle-producer-archivelog.env
docker compose up -d --build
docker compose logs -f producer consumer
```

Перед запуском проверь `BROKER`/`KAFKA_BROKER` в сервисных env-файлах из директории `env/`.

## Особенности

- producer: `enable.idempotence=true`, `acks=all`
- consumer: `enable.auto.commit=false`
- commit offset только после успешной обработки
- при ошибке обработки commit не делается (сообщение перечитается)

## Режимы работы (через env)

- `PRODUCER_RUN_MODE=continuous|interval|oneshot`
- `CONSUMER_RUN_MODE=continuous|interval|oneshot`

Где менять:

- producer: `env/producer.env`
- consumer: `env/consumer.env`

Рекомендация:

- `consumer`: `continuous`
- `producer` для БД-источника: `oneshot` + внешний scheduler (cron/Kubernetes CronJob/Airflow)

Полезные параметры:

- producer: `PRODUCER_INTERVAL_SEC`, `PRODUCER_BATCH_SIZE`
- consumer: `CONSUMER_INTERVAL_SEC`, `CONSUMER_BATCH_SIZE`, `CONSUMER_BATCH_TIMEOUT_SEC`

Примеры:

```bash
# continuous (дефолт)
docker compose up -d --build

# oneshot producer (один проход)
docker compose run --rm -e PRODUCER_RUN_MODE=oneshot producer

# interval consumer (каждые 30 сек)
docker compose run --rm -e CONSUMER_RUN_MODE=interval -e CONSUMER_INTERVAL_SEC=30 consumer
```

## Oracle -> Kafka producer (LogMiner)

В эту же контейнерную папку добавлены скрипты:

- `app/oracle_to_kafka_producer_archivelog.py` (вариант через `ADD_LOGFILE` из `v$archived_log`)

Запускать его лучше вручную (он не стартует постоянно через `up -d`):

```bash
docker compose run --rm oracle-producer-archivelog
```

Подробная инструкция по archived-log версии:

- `README_ORACLE_ARCHIVELOG.md`

Логика:

- читает Oracle LogMiner (`V$LOGMNR_CONTENTS`)
- пишет события в `KAFKA_TOPIC` (по умолчанию `oracle.logminer.raw`)
- хранит watermark в `./state/oracle_kafka_state_archivelog.json`

## TLS сертификат

Положи CA сертификат в `./certs/ca.crt` перед запуском.

Пример:

```bash
cp ../apache-kafka-stack/scripts/tls/ca.crt ./certs/ca.crt
```

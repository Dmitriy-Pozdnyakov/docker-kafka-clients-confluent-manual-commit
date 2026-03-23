# Docker Kafka Clients (Confluent Manual Commit)

Текущий проект разделен на 2 CDC-процесса:

1. `app/oracle_cdc_schema_build` — генерация и регистрация схем.
2. `app/oracle_cdc_producer` — producer LogMiner -> Kafka (SR CDC envelope).

## Подготовка env

```bash
cp app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env.example \
   app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env

cp app/oracle_cdc_schema_build/env/oracle-schema-build.env.example \
   app/oracle_cdc_schema_build/env/oracle-schema-build.env
```

## Процесс 1: создание схем

```bash
docker compose run --rm oracle-schema-build
```

Скрипт:
- `app/oracle_cdc_schema_build/build_schemas_from_oracle.py`

Схемы складываются в:
- `schemas`

## Процесс 2: запуск producer

```bash
docker compose run --rm oracle-producer-archivelog-sr
```

Скрипт:
- `app/oracle_cdc_producer/producer_archivelog_sr.py`

Roadmap и журнал доработок:
- `road-map.md`

Producer читает схемы из общей директории:
- `schemas`

## TLS сертификат

Положи CA сертификат в `./certs/ca.crt` перед запуском.

```bash
cp ../apache-kafka-stack/scripts/tls/ca.crt ./certs/ca.crt
```

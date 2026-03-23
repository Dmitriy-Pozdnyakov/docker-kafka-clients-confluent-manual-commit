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

## Разделение Oracle-кредов

В проекте используются 2 независимых набора Oracle-кредов:

1. `oracle-producer-archivelog-sr` (LogMiner runtime):
- пользователь/пароль для чтения redo stream (`C##LOGMINER`);
- DSN: `.../FREE` (CDB service).

2. `oracle-schema-build` (metadata build):
- пользователь/пароль для чтения метаданных таблиц (`ALL_TAB_COLUMNS`/PK);
- DSN: `.../FREEPDB1` (PDB service, где находятся `HR.*` таблицы).

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

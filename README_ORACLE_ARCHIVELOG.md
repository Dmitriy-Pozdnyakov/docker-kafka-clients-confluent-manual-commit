# Oracle LogMiner (Archived Logs) -> Kafka

Инструкция для версии продюсера:

- `app/oracle_to_kafka_producer_archivelog.py` (контейнерный/env-вариант)
- `app/oracle_to_kafka_producer_archivelog_jupyter.py` (jupyter-вариант)

## 1) Что делает скрипт

1. Читает `v$archived_log`, выбирает последние archived logs.
2. Запускает `DBMS_LOGMNR.START_LOGMNR`.
3. Читает изменения из `v$logmnr_contents` (DML: insert/update/delete).
4. Пишет события в Kafka topic.
5. Сохраняет watermark (`last_commit_scn`) в state-файл.

## 2) Запуск контейнерного варианта

Подготовь env-файл сервиса:

```bash
cp env/oracle-producer-archivelog.env.example env/oracle-producer-archivelog.env
```

```bash
docker compose run --rm oracle-producer-archivelog
```

## 3) Основные env-параметры (контейнерный вариант)

- Oracle:
  - `ORACLE_USER`
  - `ORACLE_PASSWORD`
  - `ORACLE_DSN`
  - `ARCHIVE_LOG_LIMIT` (по умолчанию `3`)
  - `CALL_TIMEOUT_MS` (по умолчанию `30000`)
- Kafka:
  - `KAFKA_BROKER`
  - `KAFKA_TOPIC` (по умолчанию `oracle.logminer.raw`)
  - `KAFKA_SECURITY_PROTOCOL` (`PLAINTEXT|SSL|SASL_SSL|SASL_PLAINTEXT`)
  - `SSL_CAFILE`, `SSL_CHECK_HOSTNAME`
  - `KAFKA_SASL_MECHANISM`, `KAFKA_SASL_USERNAME`, `KAFKA_SASL_PASSWORD`
- State/loop:
  - `STATE_FILE`
  - `START_FROM_SCN`
  - `POLL_SECONDS`
- Batch/control:
  - `MAX_ROWS_PER_BATCH` (по умолчанию `5000`, `0` = без лимита)
  - `PRODUCE_RETRY_TIMEOUT_SEC` (по умолчанию `60`)
  - `PRODUCE_RETRY_POLL_SEC` (по умолчанию `0.2`)
  - `LOG_FIRST_N_EVENTS` (по умолчанию `3`)
  - `TOPIC_PER_TABLE` (`true|false`, по умолчанию `false`)
  - `TOPIC_PREFIX` (префикс в режиме topic per table, например `oracle.cdc`)
  - `TOPIC_SEPARATOR` (разделитель частей topic name, по умолчанию `.`)

## 4) Фильтрация: одна схема/таблица или список

Поддерживаются оба варианта:

- старый (одиночный):
  - `FILTER_SCHEMA=HR`
  - `FILTER_TABLE=ORDERS`
- новый (список):
  - `FILTER_SCHEMAS=HR,SALES,FIN`
  - `FILTER_TABLES=ORDERS,CUSTOMERS`

Можно комбинировать. Скрипт объединяет значения и убирает дубли.

Пример:

```bash
docker compose run --rm \
  -e FILTER_SCHEMAS=HR,SALES \
  -e FILTER_TABLES=ORDERS,CUSTOMERS \
  -e MAX_ROWS_PER_BATCH=2000 \
  oracle-producer-archivelog
```

## 5) Jupyter-вариант

```python
from app.oracle_to_kafka_producer_archivelog_jupyter import Config, run_once

cfg = Config(
    oracle_user="...",
    oracle_password="...",
    oracle_dsn="host:1521/service",
    kafka_broker="10.20.30.40:19092,10.20.30.40:19093,10.20.30.40:19094",
    kafka_topic="oracle.logminer.raw",
    max_rows_per_batch=2000,
    filter_schemas=("HR", "SALES"),
    filter_tables=("ORDERS", "CUSTOMERS"),
)

stats = run_once(cfg)
print(stats)
```

Также остаются поля для одиночного фильтра:

- `filter_schema="HR"`
- `filter_table="ORDERS"`

## 6) Поведение батча

- `run_once(...)` обрабатывает до `max_rows_per_batch`.
- Остальные изменения будут обработаны на следующем запуске.
- При переполнении локальной очереди producer используется retry с таймаутом (без бесконечного цикла).

## 7) Topic Per Table

Если нужен отдельный topic на каждую таблицу:

```bash
docker compose run --rm \
  -e TOPIC_PER_TABLE=true \
  -e TOPIC_PREFIX=oracle.cdc \
  -e TOPIC_SEPARATOR=. \
  oracle-producer-archivelog
```

Формат topic:

- `oracle.cdc.<schema>.<table>` (с нормализацией в lower-case)

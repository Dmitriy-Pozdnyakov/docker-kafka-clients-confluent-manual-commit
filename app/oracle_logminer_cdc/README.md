# Oracle LogMiner CDC + Schema Registry

Этот документ описывает сценарий:
`app/oracle_logminer_cdc/producer_archivelog_sr.py`

Цель:
- сохранить one-shot модель (под `cron + flock`);
- добавить CDC envelope + Schema Registry (опционально);
- не ломать стабильный сценарий `oracle_to_kafka_producer_archivelog.py`.

## Что есть сейчас

1. Kafka auth/SSL поддержаны так же, как в стабильном скрипте.
2. Для Schema Registry используется только URL (`SCHEMA_REGISTRY_URL`).
3. CDC/SR логика вынесена в отдельный модуль:
   `app/oracle_logminer_cdc/cdc_schema_registry.py`.
4. Запуск one-shot: скрипт делает 1 батч и завершает процесс.

## Что не покрыто (важно)

1. SQL parser в модуле прототипный и консервативный.
2. Для сложных `sql_redo/sql_undo` возможен fallback в raw mode (если `CDC_PARSE_ERROR_MODE=raw`).
3. Полноценный SR auth/SSL для Registry пока не добавлен.

## Быстрый запуск в Docker

Подготовь персональный env для этого сценария:

```bash
cp app/oracle_logminer_cdc/oracle-producer-archivelog-sr.env.example \
   app/oracle_logminer_cdc/oracle-producer-archivelog-sr.env
```

Запуск через выделенный сервис `oracle-producer-archivelog-sr`:

```bash
docker compose run --rm oracle-producer-archivelog-sr
```

Если нужны дополнительные переменные (CDC/SR), удобно передавать через `-e`:

```bash
docker compose run --rm \
  -e CDC_ENVELOPE_ENABLED=true \
  -e TOPIC_PER_TABLE=true \
  -e SCHEMA_REGISTRY_URL=http://10.20.30.40:8081 \
  -e SCHEMA_DIR=/app/schemas \
  oracle-producer-archivelog-sr
```

## Минимальные env

Базовые (как в стабильном скрипте):
- `ORACLE_USER`
- `ORACLE_PASSWORD`
- `ORACLE_DSN`
- `KAFKA_BROKER` (или `BROKER`)
- `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_*`, `SSL_CAFILE` (по необходимости)

CDC/SR:
- `CDC_ENVELOPE_ENABLED` (`true|false`)
- `CDC_SUPPORTED_TABLES` (пример: `HR.EMP,HR.DEPT`)
- `CDC_KEY_MODE` (`technical|pk`)
- `CDC_PARSE_ERROR_MODE` (`raw|fail`)
- `SCHEMA_REGISTRY_URL` (пример: `http://10.20.30.40:8081`)
- `SCHEMA_DIR` (путь до JSON-схем внутри контейнера)
- `SCHEMA_AUTO_REGISTER`, `SCHEMA_USE_LATEST_VERSION`, `SCHEMA_NORMALIZE`

Рекомендуемый manual режим для SR:
- `SCHEMA_AUTO_REGISTER=false`
- схемы предварительно регистрируются в Registry (через CI/CD или отдельный скрипт).

Batch/state:
- `STATE_FILE` (рекомендуется `/state/...`)
- `ARCHIVE_LOG_LIMIT`
- `MAX_ROWS_PER_BATCH`
- `USE_FETCHMANY`
- `FETCHMANY_SIZE`

## Формат schema файлов

Для topic `oracle.cdc.hr.employees` ожидаются:

1. `schemas/oracle.cdc.hr.employees.key.json`
2. `schemas/oracle.cdc.hr.employees.value.json`

Именно по topic вычисляется путь к файлам схем.

## Генератор schema файлов

В папке есть генератор:
`app/oracle_logminer_cdc/generate_json_schemas.py`

Примеры:

```bash
# Вариант 1: передать готовые topic'и
python app/oracle_logminer_cdc/generate_json_schemas.py \
  --topics oracle.cdc.hr.employees,oracle.cdc.hr.departments

# Вариант 2: передать таблицы SCHEMA.TABLE
python app/oracle_logminer_cdc/generate_json_schemas.py \
  --tables HR.EMPLOYEES,HR.DEPARTMENTS \
  --topic-prefix oracle.cdc
```

По умолчанию файлы создаются в:
`app/oracle_logminer_cdc/schemas`

Если нужно перезаписать существующие файлы:

```bash
python app/oracle_logminer_cdc/generate_json_schemas.py \
  --topics oracle.cdc.hr.employees \
  --overwrite
```

## Генерация схем из Oracle metadata

Для production-подхода используй отдельный скрипт:
`app/oracle_logminer_cdc/build_schemas_from_oracle.py`

Он читает `ALL_TAB_COLUMNS` + PK и строит реальные schema файлы под таблицы.

Примеры:

```bash
# Сгенерировать схемы для 2 таблиц
python app/oracle_logminer_cdc/build_schemas_from_oracle.py \
  --oracle-user "$ORACLE_USER" \
  --oracle-password "$ORACLE_PASSWORD" \
  --oracle-dsn "$ORACLE_DSN" \
  --tables HR.EMPLOYEES,HR.DEPARTMENTS \
  --topic-prefix oracle.cdc \
  --schema-dir app/oracle_logminer_cdc/schemas \
  --key-mode technical \
  --overwrite

# То же + регистрация в SR (manual pre-deploy шаг)
python app/oracle_logminer_cdc/build_schemas_from_oracle.py \
  --oracle-user "$ORACLE_USER" \
  --oracle-password "$ORACLE_PASSWORD" \
  --oracle-dsn "$ORACLE_DSN" \
  --tables HR.EMPLOYEES,HR.DEPARTMENTS \
  --topic-prefix oracle.cdc \
  --schema-dir app/oracle_logminer_cdc/schemas \
  --register-sr \
  --sr-url http://10.20.30.40:8081
```

Через Docker Compose (one-shot сервис):

```bash
cp app/oracle_logminer_cdc/oracle-schema-build.env.example \
   app/oracle_logminer_cdc/oracle-schema-build.env

docker compose run --rm oracle-schema-build
```

Флаги `--overwrite` и `--register-sr` управляются через env:
- `OVERWRITE=` (пусто) -> без флага
- `OVERWRITE=1` -> добавить `--overwrite`
- `REGISTER_SR=` (пусто) -> без флага
- `REGISTER_SR=1` -> добавить `--register-sr`

## Рекомендуемый rollout

1. Запуск с `CDC_ENVELOPE_ENABLED=false` и проверка стабильности батча.
2. Включить `TOPIC_PER_TABLE=true`.
3. Добавить схемы для 1 таблицы и включить `CDC_ENVELOPE_ENABLED=true`.
4. Проверить данные в SR + Kafka.
5. Расширить на остальные таблицы.

## Структура модулей

Сейчас уже выделены:

1. `app/oracle_logminer_cdc/config.py`:
   - `Config`, `load_config_from_env`, `validate_config`, `merge_name_filters`.

2. `app/oracle_logminer_cdc/cdc_schema_registry.py`:
   - `SchemaRuntime`, parser SQL_REDO/SQL_UNDO, `build_cdc_event`.

3. В `app/oracle_logminer_cdc/producer_archivelog_sr.py` пока остаются runtime-части:
   - `_end_logminer`, `_add_logfile`, `_start_logminer`,
   - `_get_archived_logs`, `_build_fetch_rows_query`, `_record_to_row`.

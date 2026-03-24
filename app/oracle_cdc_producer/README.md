# Oracle CDC Producer

Этот модуль отвечает за producer-процесс:
- чтение изменений из Oracle LogMiner;
- формирование CDC envelope;
- публикация в Kafka (Schema Registry JSON).

Важно:
- этот producer работает в strict CDC-only режиме (`CDC_ENVELOPE_ENABLED=true`);
- raw режим вынесен в отдельный модуль `app/oracle_raw_producer`.

## Точка входа и запуск

Запускается сервисом `oracle-producer-archivelog-sr` из `docker-compose.yaml`:

```bash
docker compose run --rm oracle-producer-archivelog-sr
```

Что вызывается под капотом:
- entrypoint: `python app/oracle_cdc_producer/producer_archivelog_sr.py`;
- env: `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env`.

## Зависимости Python

Пакеты задаются в корневом `requirements.txt`.

| Пакет | Где используется | Для чего |
|---|---|---|
| `oracledb` | `producer_archivelog_sr.py`, `cdc_schema_registry.py` | Подключение к Oracle, чтение LogMiner и метаданных таблиц |
| `confluent-kafka` | `producer_archivelog_sr.py`, `cdc_schema_registry.py` | Kafka Producer + JSON Serializer/Schema Registry client |
| `sqlglot` (optional) | `cdc_sql_parser_sqlglot.py` | AST-парсинг SQL_REDO/SQL_UNDO для backend `sqlglot/auto` |
| `jsonschema`, `referencing`, `httpx`, `attrs`, `cachetools`, `authlib` | транзитивно для SR-части | Работа с Schema Registry и JSON Schema стэком |

Важно:
- без `sqlglot` producer работает, но parser backend будет legacy-only;
- SR-зависимости обязательны, т.к. этот runner работает только в CDC + SR режиме.

## Что откуда вызывается (Python-модули)

| Модуль/скрипт | Кто вызывает | Что делает |
|---|---|---|
| `producer_archivelog_sr.py` | Docker Compose command | Основной runtime: config, LogMiner batch, publish, state |
| `config.py` | `producer_archivelog_sr.py` | Загрузка env и валидация параметров |
| `cdc_schema_registry.py` | `producer_archivelog_sr.py` | Построение CDC envelope, key/value, работа с SR serializer |
| `cdc_sql_parser.py` | `cdc_schema_registry.py` | Фасад парсинга: выбор backend + fallback + type coercion |
| `cdc_sql_parser_legacy.py` | `cdc_sql_parser.py` | Regex/parser backend |
| `cdc_sql_parser_sqlglot.py` | `cdc_sql_parser.py` | AST backend на `sqlglot` |
| `stress_test_cdc_sql_parser.py` | вручную из CLI | Нагрузочная и error-focused проверка parser логики |

## Мини-схема потока

```text
docker compose run --rm oracle-producer-archivelog-sr
  -> producer_archivelog_sr.py
     -> config.load_config_from_env() + validate_config()
     -> Oracle (v$archived_log, DBMS_LOGMNR.START_LOGMNR, v$logmnr_contents)
     -> cdc_schema_registry.build_cdc_event()
        -> cdc_sql_parser.parse_cdc_row_images()
           -> legacy parser and/or sqlglot parser
        -> SchemaRuntime(JSONSerializer)
           -> reads /app/schemas/<topic>.key.json
           -> reads /app/schemas/<topic>.value.json
           -> registers/fetches schema in Schema Registry
     -> confluent_kafka.Producer.produce() -> Kafka topic(s)
     -> save STATE_FILE (/state/*.json)
```

## Cron pipeline без make

Рекомендуемая схема:
- `oracle-schema-build` по редкому расписанию (например, раз в ночь или после DDL);
- `oracle-producer-archivelog-sr` чаще (например, каждую минуту).

Готовый скрипт:
- `scripts/cron_cdc.sh`
- режимы: `producer`, `schema` (`schema-build`), `pipeline`

Примеры режимов:

`<project-dir>` в примерах ниже — путь к каталогу `docker-kafka-confluent-manual-commit`.

Тестовый режим (интеграционный прогон, вручную):

```bash
cd <project-dir>
SCHEMA_OVERWRITE=yes SCHEMA_REGISTER_SR= ./scripts/cron_cdc.sh pipeline
```

Тестовый режим через cron (например, каждые 15 минут):

```cron
*/15 * * * * SCHEMA_OVERWRITE=yes SCHEMA_REGISTER_SR= \
  <project-dir>/scripts/cron_cdc.sh pipeline
```

Прод-режим через cron (рекомендуется 2 отдельные записи):

```cron
# producer: часто (пример: каждую минуту)
* * * * * <project-dir>/scripts/cron_cdc.sh producer

# schema-build: редко (пример: раз в день), опционально с регистрацией в SR
5 2 * * * SCHEMA_OVERWRITE=yes SCHEMA_REGISTER_SR=yes \
  <project-dir>/scripts/cron_cdc.sh schema
```

Что делает скрипт:
- сам переходит в корень проекта;
- использует `flock` (Linux) для lock-файла `/tmp/oracle-cdc.lock` и не допускает параллельный запуск;
- пишет логи в `state/cron-logs/*.log`;
- поддерживает env-overrides: `SCHEMA_OVERWRITE`, `SCHEMA_REGISTER_SR`, `SCHEMA_DSN`, `PRODUCER_DSN`, `LOCK_FILE`, `LOG_FILE`.

Перед этим убедитесь, что builder и producer используют согласованные topic/key/schema параметры.

Важно по Oracle DSN:
- креды/DSN producer используются только для LogMiner runtime (`oracle-producer-archivelog-sr`);
- для producer используем `ORACLE_DSN=.../FREE` и пользователя `C##LOGMINER`;
- schema-builder использует отдельные креды и отдельный DSN `.../FREEPDB1` (где живут `HR.*` таблицы).

Примечание по SR:
- если у `oracle-schema-build` включен `REGISTER_SR=yes`, возможен `409 Conflict` при несовместимой эволюции схем в Schema Registry (это не связано с DSN).
- в текущей версии schema-build регистрация сделана idempotent: при `409` и совпадающей latest схеме subject помечается как `already-registered` (не ошибка).

## SR Preflight в producer

При `CDC_ENVELOPE_ENABLED=true` producer перед отправкой CDC-событий делает preflight:
- проверяет наличие локальных schema-файлов `<topic>.key.json` и `<topic>.value.json`;
- проверяет совместимость subject-ов `<topic>-key` и `<topic>-value` через SR compatibility API.

Сценарии запуска:
- single-topic: preflight выполняется до начала batch;
- topic-per-table + `CDC_SUPPORTED_TABLES`: preflight выполняется заранее для всех topic из whitelist;
- topic-per-table без whitelist: preflight делается лениво на first-use topic.

## Parser backend режимы

- `auto` (по умолчанию): сначала `sqlglot`, затем fallback на legacy parser;
- `auto_legacy_first` (alias `legacy_first`): сначала legacy, если не справился -> `sqlglot`;
- `sqlglot`: только sqlglot backend;
- `legacy`: только regex/parser backend.

Управляется переменной:
- `CDC_SQL_PARSER_BACKEND=auto|auto_legacy_first|legacy_first|legacy|sqlglot`

Схемы для сериализации читаются из:
- `schemas` (в контейнере путь `/app/schemas`).

## Проверка parser (стресс + ошибки)

Прогон с дефолтами:

```bash
python3 app/oracle_cdc_producer/stress_test_cdc_sql_parser.py
```

Более тяжелый прогон:

```bash
python3 app/oracle_cdc_producer/stress_test_cdc_sql_parser.py --iterations 50000 --invalid-ratio 0.30 --seed 42
```

Проверка конкретного backend:

```bash
python3 app/oracle_cdc_producer/stress_test_cdc_sql_parser.py --backend legacy
python3 app/oracle_cdc_producer/stress_test_cdc_sql_parser.py --backend auto_legacy_first
python3 app/oracle_cdc_producer/stress_test_cdc_sql_parser.py --backend sqlglot
```

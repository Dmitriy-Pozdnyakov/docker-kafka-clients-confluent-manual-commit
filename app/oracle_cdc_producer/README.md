# Oracle CDC Producer

Этот модуль отвечает за producer-процесс:
- чтение изменений из Oracle LogMiner;
- формирование CDC envelope;
- публикация в Kafka (Schema Registry JSON).

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
- SR-зависимости нужны, когда `CDC_ENVELOPE_ENABLED=true`.

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

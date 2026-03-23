# Oracle LogMiner CDC Road Map

## Цель
Сделать текущий CDC-пайплайн устойчивым для регулярной эксплуатации: предсказуемый формат событий, контролируемые ошибки, понятная наблюдаемость и повторяемый процесс доработок.

## Как ведем этот файл
1. Держим roadmap как живой backlog (что делать дальше).
2. После каждой доработки добавляем запись в `Журнал выполненных доработок`.
3. Запись должна быть короткой и практичной:
   - что сделано;
   - краткое описание результата;
   - какие файлы затронуты;
   - чем проверили.

## Backlog по этапам

### Этап 1. Быстрый hardening
- [ ] Атомарное сохранение `STATE_FILE` (temp + fsync + rename).
- [ ] Fail-fast валидация Kafka security-конфига (SASL/SSL обязательные параметры).
- [ ] Выравнивание `*.env` и `*.env.example` для SR-режима (DSN, broker, `SCHEMA_DIR`, SR URL).

### Этап 2. Качество CDC payload
- [ ] Нормализация DATE/TIMESTAMP в canonical формат (ISO-8601 UTC) для `before/after`.
- [ ] Явный маршрут ошибок парсинга (`CDC_PARSE_ERROR_TOPIC`/DLQ), без неявной деградации.
- [ ] Строгий режим контрактов: при schema mismatch не продвигать `commit_scn`.

### Этап 3. Тестируемость
- [ ] Unit-тесты SQL parser (INSERT/UPDATE/DELETE, кавычки, `AND`, вложенные функции).
- [ ] Набор golden fixtures для `sql_redo/sql_undo` из реальных логов.
- [ ] Интеграционный smoke test: Oracle -> producer -> Kafka (проверка key/value формата).

### Этап 4. Schema Registry и контракт
- [ ] Полная SR-конфигурация в runtime (auth/ssl parity с schema-builder).
- [ ] Зафиксировать единую стратегию источника схем (локальные файлы vs SR latest).
- [ ] Preflight-проверка совместимости subject-ов перед запуском батча.

### Этап 5. Эксплуатация и мониторинг
- [ ] Структурированные логи (JSON) + стабильные поля для алертов.
- [ ] Метрики: `rows`, `delivered`, `failed`, `cdc_rows`, `raw_rows`, `cdc_fallback_raw`, `last_commit_scn`.
- [ ] Короткий runbook восстановления после ошибок (state drift, parser fail spike).

## Журнал выполненных доработок

| Дата | Что выполнено | Краткое описание результата | Файлы | Проверка |
|---|---|---|---|---|
| 2026-03-21 | Создан `road-map.md` | Добавлен поэтапный backlog и шаблон журнала фиксации доработок | `road-map.md` | Визуальная проверка файла |
| 2026-03-23 | Разделение структуры проекта | Старые не-CDC сценарии были вынесены отдельно, а CDC часть оставлена в выделенном каталоге producer | `docker-compose.yaml`, `README.md`, `Dockerfile` | `docker compose config`, `python3 -m py_compile` |
| 2026-03-23 | Cleanup после удаления старых сценариев | Удалены оставшиеся ссылки в compose/docs/env, проект приведен к чистому CDC-only состоянию | `docker-compose.yaml`, `README.md`, `.env`, `.env.example`, `Dockerfile` | `docker compose config` |
| 2026-03-23 | Разделение CDC на 2 процесса | Producer и schema-build вынесены в разные папки (`app/oracle_cdc_producer`, `app/oracle_cdc_schema_build`), обновлены compose/env/default paths | `docker-compose.yaml`, `README.md`, `app/oracle_cdc_producer/*`, `app/oracle_cdc_schema_build/*`, `.gitignore` | `docker compose config`, `python3 -m py_compile` |
| 2026-03-23 | Структурирование env по приложениям | Env-файлы перенесены в подпапки `env/` внутри каждого приложения, обновлены пути в compose и документации | `docker-compose.yaml`, `README.md`, `app/oracle_cdc_producer/env/*`, `app/oracle_cdc_schema_build/env/*` | `docker compose config` |
| 2026-03-23 | Вынос schemas в корень проекта | Schema-файлы перенесены в `./schemas` (на уровень с `certs`), обновлены volume/env/default пути на `/app/schemas` | `docker-compose.yaml`, `.gitignore`, `schemas/*`, `app/*/env/*`, `README.md` | `docker compose config`, `rg` проверка путей |
| 2026-03-23 | Перенос roadmap в корень проекта | `road-map.md` перемещен на уровень `docker-compose.yaml`, ссылки в документации синхронизированы | `road-map.md`, `README.md` | `rg` проверка ссылок |
| 2026-03-23 | Вынесение SQL parser в отдельный модуль | Parser SQL_REDO/SQL_UNDO выделен в `cdc_sql_parser.py`, `cdc_schema_registry.py` оставлен как orchestration-слой | `app/oracle_cdc_producer/cdc_sql_parser.py`, `app/oracle_cdc_producer/cdc_schema_registry.py`, `app/oracle_cdc_producer/README.md` | `python3 -m py_compile`, `rg` проверка импортов |
| 2026-03-23 | Удаление legacy-фильтров из producer config | Оставлен только CSV-подход (`FILTER_SCHEMAS`/`FILTER_TABLES`), удалена поддержка `FILTER_SCHEMA`/`FILTER_TABLE` и merge helper | `app/oracle_cdc_producer/config.py`, `app/oracle_cdc_producer/producer_archivelog_sr.py`, `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env`, `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env.example`, `road-map.md` | `python3 -m py_compile`, `rg -n \"FILTER_SCHEMA|FILTER_TABLE|merge_name_filters\"` |
| 2026-03-23 | Документирование блоков во всех env-файлах | Добавлены единые секции и пояснения по назначению параметров для compose, producer и schema-build env-файлов | `.env`, `.env.example`, `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env`, `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env.example`, `app/oracle_cdc_schema_build/env/oracle-schema-build.env`, `app/oracle_cdc_schema_build/env/oracle-schema-build.env.example`, `road-map.md` | Визуальная проверка структуры и комментариев |
| 2026-03-23 | Stress/error проверка SQL parser | Добавлен отдельный stress runner с фиксированными и random кейсами (включая ожидаемые ошибки), подтверждено что parser корректно парсит валидный SQL и падает на невалидном | `app/oracle_cdc_producer/stress_test_cdc_sql_parser.py`, `app/oracle_cdc_producer/README.md`, `road-map.md` | `python3 .../stress_test_cdc_sql_parser.py --iterations 5000`, `python3 .../stress_test_cdc_sql_parser.py --iterations 100000 --invalid-ratio 0.30`, `python3 -m py_compile` |
| 2026-03-23 | Рефактор читаемости SQL parser | Упрощена структура `cdc_sql_parser.py`: выделены небольшие helper-функции, убрано дублирование разбора top-level SQL, сохранен API и текущее поведение | `app/oracle_cdc_producer/cdc_sql_parser.py`, `road-map.md` | `python3 -m py_compile`, `python3 .../stress_test_cdc_sql_parser.py --iterations 5000`, `python3 .../stress_test_cdc_sql_parser.py --iterations 100000 --invalid-ratio 0.30` |
| 2026-03-23 | Гибридный parser backend (sqlglot + fallback) | Добавлен optional backend `auto/sqlglot/legacy`: при наличии `sqlglot` используется AST-парсинг, при ошибках/отсутствии — fallback на legacy parser | `app/oracle_cdc_producer/cdc_sql_parser.py`, `app/oracle_cdc_producer/stress_test_cdc_sql_parser.py`, `app/oracle_cdc_producer/README.md`, `requirements.txt`, `.gitignore`, `road-map.md` | `py_compile`, stress в `--backend legacy/auto/sqlglot` (sqlglot с локальным `PYTHONPATH=.tmp-pydeps`) |
| 2026-03-23 | Обновление sqlglot до актуальной major-ветки | Диапазон зависимости поднят до `30.x`, проверена совместимость parser backend `sqlglot/auto` на `sqlglot 30.0.3` | `requirements.txt`, `road-map.md` | stress в `--backend sqlglot/auto` на 5k и 20k кейсов (`PYTHONPATH=.tmp-pydeps`) |
| 2026-03-23 | Разделение parser на backend-модули | Логика разделена на `legacy` и `sqlglot` модули, `cdc_sql_parser.py` оставлен фасадом с fallback и общей type coercion; добавлены подробные комментарии по шагам разбора | `app/oracle_cdc_producer/cdc_sql_parser.py`, `app/oracle_cdc_producer/cdc_sql_parser_legacy.py`, `app/oracle_cdc_producer/cdc_sql_parser_sqlglot.py`, `app/oracle_cdc_producer/README.md`, `road-map.md` | `py_compile`, stress в `--backend legacy/auto/sqlglot` |
| 2026-03-23 | Настройка порядка fallback backend | Добавлен режим `auto_legacy_first` (alias `legacy_first`): сначала legacy parser, при ошибке fallback в sqlglot; прокинут env-параметр `CDC_SQL_PARSER_BACKEND` в runtime | `app/oracle_cdc_producer/cdc_sql_parser.py`, `app/oracle_cdc_producer/config.py`, `app/oracle_cdc_producer/cdc_schema_registry.py`, `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env`, `app/oracle_cdc_producer/env/oracle-producer-archivelog-sr.env.example`, `app/oracle_cdc_producer/stress_test_cdc_sql_parser.py`, `app/oracle_cdc_producer/README.md`, `road-map.md` | `py_compile`, stress в `--backend auto_legacy_first`, проверка `--backend legacy_first` |
| 2026-03-23 | Расширена документация producer-модуля | В README добавлены разделы по Python-зависимостям, карте модулей и цепочке вызовов, а также мини-схема потока выполнения от compose до Kafka/SR/state | `app/oracle_cdc_producer/README.md`, `road-map.md` | Визуальная проверка README, сверка с `docker-compose.yaml` и импортами Python-модулей |

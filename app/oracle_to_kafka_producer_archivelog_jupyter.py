#!/usr/bin/env python3
"""
Jupyter-friendly вариант Oracle LogMiner -> Kafka (archived logs).

Главная идея:
- можно запускать один батч через `run_once(config)`;
- можно запускать цикл через `run_loop(config)`, но это опционально.
"""

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

import oracledb
from confluent_kafka import Producer


@dataclass
class Config:
    # =========================
    # Oracle connection
    # =========================
    oracle_user: str  # пользователь Oracle
    oracle_password: str  # пароль Oracle
    oracle_dsn: str  # DSN Oracle, например "host:1521/service_name"
    call_timeout_ms: int = 30000  # timeout для Oracle-запросов в миллисекундах
    archive_log_limit: int = 3  # сколько последних archived logs подключать в LogMiner
    use_no_rowid_in_stmt: bool = True  # добавлять ли NO_ROWID_IN_STMT в опции LogMiner

    # =========================
    # Kafka connection
    # =========================
    kafka_broker: str = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"  # bootstrap servers
    kafka_topic: str = "oracle.logminer.raw"  # целевой топик
    kafka_security_protocol: str = "PLAINTEXT"  # PLAINTEXT / SSL / SASL_SSL / SASL_PLAINTEXT
    ssl_cafile: str = ""  # путь до CA сертификата (для SSL/SASL_SSL)
    ssl_check_hostname: bool = True  # проверять ли hostname в TLS-сертификате
    kafka_sasl_mechanism: str = "PLAIN"  # SASL механизм (обычно PLAIN)
    kafka_sasl_username: str = ""  # SASL username
    kafka_sasl_password: str = ""  # SASL password
    kafka_client_id: str = "oracle-logminer-jupyter"  # client.id для Kafka producer
    kafka_flush_timeout_sec: int = 30  # timeout flush producer в секундах
    produce_retry_timeout_sec: int = 60  # максимум ожидания свободного места в очереди на 1 сообщение
    produce_retry_poll_sec: float = 0.2  # как часто опрашивать producer при переполнении очереди
    topic_per_table: bool = False  # писать ли в отдельный topic на каждую таблицу
    topic_prefix: str = ""  # префикс для topic per table, например "oracle.cdc"
    topic_separator: str = "."  # разделитель частей topic name

    # =========================
    # State management
    # =========================
    state_file: str = "./oracle_kafka_state_jupyter.json"  # файл с last_commit_scn между запусками
    start_from_commit_scn: int = 0  # стартовый commit_scn, если state-файл еще не создан

    # =========================
    # Loop mode
    # =========================
    poll_seconds: int = 60  # пауза между батчами в run_loop()

    # =========================
    # Data filters (optional)
    # =========================
    filter_schema: str = ""  # фильтр по схеме Oracle (seg_owner), например "HR"
    filter_table: str = ""  # фильтр по таблице Oracle (table_name), например "EMPLOYEES"
    filter_schemas: Tuple[str, ...] = ()  # список схем, например ("HR", "SALES")
    filter_tables: Tuple[str, ...] = ()  # список таблиц, например ("ORDERS", "CUSTOMERS")
    max_rows_per_batch: int = 5000  # максимум строк из LogMiner за один run_once (0 = без лимита)

    # =========================
    # Logging
    # =========================
    verbose: bool = True  # печатать подробные логи выполнения
    log_first_n_events: int = 3  # сколько первых событий батча показывать в логах детально


def _json_default(obj: Any) -> str:
    # Сериализуем datetime в ISO-формат для JSON.
    if isinstance(obj, datetime):
        return obj.isoformat()
    # Для остальных типов оставляем строковое представление.
    return str(obj)


def _log(cfg: Config, message: str) -> None:
    # Единая точка логирования: печатаем только если включен verbose.
    if cfg.verbose:
        print(f"[oracle->kafka:jupyter] {message}")


def _normalize_name_list(values: Sequence[str]) -> List[str]:
    # Нормализуем список имен: trim + upper + удаление пустых/дубликатов.
    normalized: List[str] = []
    for value in values:
        item = str(value).strip().upper()
        if item and item not in normalized:
            normalized.append(item)
    return normalized


def _merge_name_filters(single_value: str, many_values: Sequence[str]) -> List[str]:
    # Объединяем одиночный и список фильтров с удалением дублей.
    merged = _normalize_name_list(many_values)
    single = str(single_value).strip().upper()
    if single and single not in merged:
        merged.insert(0, single)
    return merged


def _load_state(cfg: Config) -> Dict[str, int]:
    # Путь до state-файла с сохраненным last_commit_scn.
    path = Path(cfg.state_file)
    # Если файл уже есть, читаем его.
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    # Если файла нет, стартуем с конфигурационного commit_scn.
    return {"last_commit_scn": cfg.start_from_commit_scn}


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    # Путь до state-файла.
    path = Path(cfg.state_file)
    # Создаем директорию под state-файл, если ее нет.
    path.parent.mkdir(parents=True, exist_ok=True)
    # Сохраняем последний обработанный commit_scn.
    with path.open("w", encoding="utf-8") as f:
        json.dump({"last_commit_scn": last_commit_scn}, f)


def _build_kafka_producer(cfg: Config) -> Producer:
    # Нормализуем protocol в верхний регистр для единообразия.
    proto = cfg.kafka_security_protocol.upper()
    # Базовые параметры продюсера.
    conf: Dict[str, Any] = {
        "bootstrap.servers": cfg.kafka_broker,
        "client.id": cfg.kafka_client_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "security.protocol": proto,
    }

    # SSL-настройки применяем только для SSL/SASL_SSL.
    if proto in {"SSL", "SASL_SSL"}:
        if cfg.ssl_cafile:
            conf["ssl.ca.location"] = cfg.ssl_cafile
        conf["ssl.endpoint.identification.algorithm"] = (
            "https" if cfg.ssl_check_hostname else "none"
        )

    # SASL-настройки применяем только для SASL-протоколов.
    if proto in {"SASL_SSL", "SASL_PLAINTEXT"}:
        conf["sasl.mechanism"] = cfg.kafka_sasl_mechanism
        conf["sasl.username"] = cfg.kafka_sasl_username
        conf["sasl.password"] = cfg.kafka_sasl_password

    # Создаем и возвращаем объект Kafka Producer.
    return Producer(conf)


def _get_archived_logs(cur: oracledb.Cursor, limit_rows: int) -> List[str]:
    # Берем N последних archived redo log файлов из v$archived_log.
    sql = """
    SELECT name
    FROM (
      SELECT name
      FROM v$archived_log
      WHERE name IS NOT NULL
      ORDER BY sequence# DESC
    )
    WHERE ROWNUM <= :limit_rows
    """
    # Выполняем запрос с bind-параметром.
    cur.execute(sql, {"limit_rows": limit_rows})
    # Оставляем только непустые имена файлов.
    files = [r[0] for r in cur.fetchall() if r and r[0]]
    # Разворачиваем, чтобы обработка шла от более старых к более новым.
    files.reverse()
    return files


def _end_logminer(cur: oracledb.Cursor) -> None:
    # Мягко завершаем текущую сессию LogMiner (если она есть).
    # Исключения внутри PL/SQL подавляются, чтобы cleanup был безопасным.
    cur.execute(
        """
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
        """
    )


def _add_logfile(cur: oracledb.Cursor, logfile: str, first: bool) -> None:
    # Первый файл добавляем с NEW, последующие через ADDFILE.
    option = "DBMS_LOGMNR.NEW" if first else "DBMS_LOGMNR.ADDFILE"
    # Подключаем archived redo log к текущей сессии LogMiner.
    cur.execute(
        f"""
        BEGIN
          DBMS_LOGMNR.ADD_LOGFILE(
            LOGFILENAME => :logfile,
            OPTIONS     => {option}
          );
        END;
        """,
        {"logfile": logfile},
    )


def _start_logminer(cur: oracledb.Cursor, cfg: Config, log_files: List[str]) -> None:
    # Без лог-файлов запуск LogMiner невозможен.
    if not log_files:
        raise RuntimeError("No archived logs found in v$archived_log")

    # На всякий случай закрываем предыдущую сессию LogMiner.
    _end_logminer(cur)
    # Добавляем все выбранные archived logs в текущую сессию.
    for i, lf in enumerate(log_files):
        _add_logfile(cur, lf, first=(i == 0))

    # Формируем набор опций запуска LogMiner.
    options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    if cfg.use_no_rowid_in_stmt:
        options += " + DBMS_LOGMNR.NO_ROWID_IN_STMT"

    # Ставим timeout для потенциально долгого START_LOGMNR.
    cur.call_timeout = cfg.call_timeout_ms
    # Запускаем LogMiner.
    cur.execute(
        f"""
        BEGIN
          DBMS_LOGMNR.START_LOGMNR(
            OPTIONS => {options}
          );
        END;
        """
    )


def _fetch_rows(
    cur: oracledb.Cursor,
    from_commit_scn: int,
    filter_schemas: Sequence[str] = (),
    filter_tables: Sequence[str] = (),
    max_rows_per_batch: int = 0,
) -> List[Dict[str, Any]]:
    # Базовый SQL: берем только DML (INSERT/UPDATE/DELETE) и только новые commit_scn.
    sql = """
        SELECT
          commit_scn,
          scn,
          timestamp,
          seg_owner,
          table_name,
          operation,
          operation_code,
          sequence# AS redo_sequence,
          rs_id,
          ssn,
          sql_redo,
          sql_undo
        FROM v$logmnr_contents
        WHERE operation_code IN (1, 2, 3)
          AND commit_scn > :from_commit_scn
    """
    # Бинды для безопасной подстановки условий.
    binds: Dict[str, Any] = {"from_commit_scn": from_commit_scn}

    # Фильтр по списку схем.
    # Регистр нормализуем заранее на уровне Python.
    if filter_schemas:
        schema_placeholders = []
        for idx, schema_name in enumerate(filter_schemas):
            bind_name = f"filter_schema_{idx}"
            schema_placeholders.append(f":{bind_name}")
            binds[bind_name] = str(schema_name).strip().upper()
        sql += f" AND seg_owner IN ({', '.join(schema_placeholders)})"

    # Фильтр по списку таблиц.
    # Регистр нормализуем заранее на уровне Python.
    if filter_tables:
        table_placeholders = []
        for idx, table_name in enumerate(filter_tables):
            bind_name = f"filter_table_{idx}"
            table_placeholders.append(f":{bind_name}")
            binds[bind_name] = str(table_name).strip().upper()
        sql += f" AND table_name IN ({', '.join(table_placeholders)})"

    # Стабильный порядок для детерминированной обработки.
    sql += " ORDER BY commit_scn, sequence#, rs_id, ssn"

    # Лимит строк батча применяем поверх уже отсортированного набора.
    if max_rows_per_batch > 0:
        sql = f"SELECT * FROM ({sql}) WHERE ROWNUM <= :max_rows_per_batch"
        binds["max_rows_per_batch"] = int(max_rows_per_batch)

    # Выполняем запрос с итоговыми фильтрами и лимитом (если задан).
    cur.execute(sql, binds)

    # Имена колонок переводим в lower-case для удобной работы как со словарем.
    cols = [c[0].lower() for c in cur.description]
    rows: List[Dict[str, Any]] = []
    # Собираем все записи в список словарей.
    for rec in cur:
        row = dict(zip(cols, rec))
        rows.append(row)
    return rows


def _event_key(row: Dict[str, Any]) -> bytes:
    # Формируем детерминированный ключ события для Kafka.
    key = (
        f"{row.get('seg_owner','')}.{row.get('table_name','')}|"
        f"{row.get('commit_scn','')}|{row.get('redo_sequence','')}|"
        f"{row.get('rs_id','')}|{row.get('ssn','')}"
    )
    # Kafka требует bytes для key/value.
    return key.encode("utf-8")


def _sanitize_topic_part(value: str) -> str:
    # Нормализуем часть topic name:
    # - lower-case;
    # - все небезопасные символы -> "_".
    safe_chars = []
    for ch in str(value).strip().lower():
        if ch.isalnum() or ch in {"-", "_", "."}:
            safe_chars.append(ch)
        else:
            safe_chars.append("_")
    result = "".join(safe_chars).strip("._-")
    return result or "unknown"


def _resolve_topic(cfg: Config, row: Dict[str, Any]) -> str:
    # Выбираем topic:
    # - обычный режим: cfg.kafka_topic;
    # - topic per table: [prefix.]schema.table
    if not cfg.topic_per_table:
        return cfg.kafka_topic

    schema_part = _sanitize_topic_part(str(row.get("seg_owner", "")))
    table_part = _sanitize_topic_part(str(row.get("table_name", "")))

    parts: List[str] = []
    prefix = _sanitize_topic_part(cfg.topic_prefix) if cfg.topic_prefix else ""
    if prefix:
        parts.append(prefix)
    parts.extend([schema_part, table_part])
    return cfg.topic_separator.join(parts)


def _produce_with_backpressure(
    cfg: Config,
    producer: Producer,
    topic: str,
    key: bytes,
    value: bytes,
    callback,
) -> Tuple[int, float]:
    # Пытаемся отправить сообщение в локальную очередь producer.
    # Если очередь переполнена, ждем освобождения через poll(), но только до дедлайна.
    retries = 0
    waited_sec = 0.0
    started = time.monotonic()
    deadline = started + max(float(cfg.produce_retry_timeout_sec), 0.0)

    while True:
        try:
            producer.produce(topic, key=key, value=value, callback=callback)
            return retries, waited_sec
        except BufferError:
            now = time.monotonic()
            if now >= deadline:
                raise RuntimeError(
                    "Kafka local queue remained full until retry timeout "
                    f"({cfg.produce_retry_timeout_sec}s)"
                ) from None

            # Даем producer обработать delivery callbacks и освободить буфер.
            producer.poll(cfg.produce_retry_poll_sec)
            retries += 1
            waited_sec = now - started


def run_once(cfg: Config) -> Dict[str, Any]:
    """
    Выполняет один цикл:
    - читает state,
    - запускает LogMiner на archived logs,
    - публикует изменения в Kafka,
    - обновляет state.
    """
    merged_schemas = _merge_name_filters(cfg.filter_schema, cfg.filter_schemas)
    merged_tables = _merge_name_filters(cfg.filter_table, cfg.filter_tables)

    # 1) Загружаем состояние: от какого commit_scn продолжаем чтение.
    state = _load_state(cfg)
    last_commit_scn = int(state.get("last_commit_scn", cfg.start_from_commit_scn))
    _log(
        cfg,
        (
            "start batch "
            f"(topic_mode={'per_table' if cfg.topic_per_table else 'single'}, "
            f"base_topic={cfg.kafka_topic}, from_commit_scn={last_commit_scn}, "
            f"filter_schemas={','.join(merged_schemas) if merged_schemas else '*'}, "
            f"filter_tables={','.join(merged_tables) if merged_tables else '*'})"
        ),
    )

    # 2) Инициализируем Kafka producer и счетчики delivery.
    producer = _build_kafka_producer(cfg)
    delivered = 0
    failed = 0
    queue_full_retries = 0
    queue_full_wait_sec = 0.0
    topics_used: Dict[str, int] = {}
    t0 = time.monotonic()
    timings: Dict[str, float] = {}

    def on_delivery(err, _msg):
        # Callback от librdkafka: считаем успешные/ошибочные отправки.
        nonlocal delivered, failed
        if err is not None:
            failed += 1
        else:
            delivered += 1

    conn: Optional[oracledb.Connection] = None
    cur: Optional[oracledb.Cursor] = None
    try:
        # 3) Подключаемся к Oracle.
        t_connect_start = time.monotonic()
        conn = oracledb.connect(
            user=cfg.oracle_user,
            password=cfg.oracle_password,
            dsn=cfg.oracle_dsn,
        )
        timings["oracle_connect_sec"] = time.monotonic() - t_connect_start
        _log(cfg, f"connected to Oracle DSN={cfg.oracle_dsn}")
        cur = conn.cursor()

        # 4) Быстрая проверка, что есть доступ к v$logmnr_contents.
        cur.execute("SELECT COUNT(*) FROM v$logmnr_contents WHERE 1 = 0")
        _ = cur.fetchone()
        _log(cfg, "access check to v$logmnr_contents passed")

        # 5) Берем archived logs и запускаем LogMiner.
        t_logs_start = time.monotonic()
        logs = _get_archived_logs(cur, cfg.archive_log_limit)
        timings["select_archived_logs_sec"] = time.monotonic() - t_logs_start
        _log(cfg, f"archived logs selected: {len(logs)}")
        for lf in logs:
            _log(cfg, f"  logfile: {lf}")
        t_logminer_start = time.monotonic()
        _start_logminer(cur, cfg, logs)
        timings["start_logminer_sec"] = time.monotonic() - t_logminer_start
        _log(cfg, "LogMiner started")

        # 6) Читаем изменения из LogMiner с учетом state и фильтров.
        t_fetch_start = time.monotonic()
        rows = _fetch_rows(
            cur,
            from_commit_scn=last_commit_scn,
            filter_schemas=merged_schemas,
            filter_tables=merged_tables,
            max_rows_per_batch=cfg.max_rows_per_batch,
        )
        timings["fetch_rows_sec"] = time.monotonic() - t_fetch_start
        _log(cfg, f"rows fetched from v$logmnr_contents: {len(rows)}")

        # 7) Публикуем каждую запись в Kafka.
        t_produce_start = time.monotonic()
        for idx, row in enumerate(rows):
            payload = json.dumps(row, ensure_ascii=False, default=_json_default).encode("utf-8")
            topic = _resolve_topic(cfg, row)
            retries, waited = _produce_with_backpressure(
                cfg=cfg,
                producer=producer,
                topic=topic,
                key=_event_key(row),
                value=payload,
                callback=on_delivery,
            )
            queue_full_retries += retries
            queue_full_wait_sec += waited
            topics_used[topic] = topics_used.get(topic, 0) + 1
            # Обрабатываем внутреннюю очередь callback.
            producer.poll(0)
            # Детальный лог только для первых N событий батча.
            if idx < cfg.log_first_n_events:
                _log(
                    cfg,
                    (
                        "queued event "
                        f"topic={topic} "
                        f"commit_scn={row.get('commit_scn')} "
                        f"schema={row.get('seg_owner')} table={row.get('table_name')} "
                        f"operation={row.get('operation')}"
                    ),
                )

        # 8) Дожидаемся отправки всех накопленных сообщений.
        t_flush_start = time.monotonic()
        producer.flush(cfg.kafka_flush_timeout_sec)
        timings["produce_loop_sec"] = time.monotonic() - t_produce_start
        timings["flush_sec"] = time.monotonic() - t_flush_start
        _log(
            cfg,
            (
                "kafka flush done "
                f"(delivered={delivered}, failed={failed}, queued={len(rows)}, "
                f"queue_full_retries={queue_full_retries}, "
                f"queue_full_wait_sec={queue_full_wait_sec:.3f})"
            ),
        )

        # 9) Если есть ошибки доставки, считаем батч неуспешным.
        if failed > 0:
            raise RuntimeError(f"Kafka delivery failures: {failed} of {len(rows)}")

        # 10) Обновляем state последним commit_scn (watermark), если были строки.
        if rows:
            new_commit_scn = max(int(r["commit_scn"]) for r in rows if r.get("commit_scn") is not None)
            last_commit_scn = max(last_commit_scn, new_commit_scn)
            _save_state(cfg, last_commit_scn)
            _log(cfg, f"state updated: last_commit_scn={last_commit_scn}")
        else:
            _log(cfg, "no new rows, state not changed")

        # 11) Возвращаем статистику батча для ноутбука/мониторинга.
        timings["total_sec"] = time.monotonic() - t0
        result = {
            "rows": len(rows),
            "delivered": delivered,
            "failed": failed,
            "queue_full_retries": queue_full_retries,
            "queue_full_wait_sec": round(queue_full_wait_sec, 3),
            "last_commit_scn": last_commit_scn,
            "logs_used": logs,
            "topics_used": topics_used,
            "timings_sec": {k: round(v, 3) for k, v in timings.items()},
        }
        _log(cfg, f"batch finished: {result}")
        return result
    finally:
        # 12) Гарантированный cleanup: LogMiner -> cursor -> connection -> producer flush.
        try:
            if cur is not None:
                try:
                    _end_logminer(cur)
                    _log(cfg, "LogMiner ended")
                except Exception:
                    pass
                cur.close()
        finally:
            if conn is not None:
                conn.close()
                _log(cfg, "Oracle connection closed")
            producer.flush(cfg.kafka_flush_timeout_sec)


def run_loop(cfg: Config) -> None:
    """Бесконечный цикл для запуска вне ноутбука (опционально)."""
    # Цикл периодически запускает run_once и спит poll_seconds.
    while True:
        try:
            stats = run_once(cfg)
            print("[oracle->kafka:jupyter] batch", stats)
        except Exception as exc:
            # Ошибка не завершает процесс: логируем и идем на следующую итерацию.
            print("[oracle->kafka:jupyter] ERROR", exc)
        time.sleep(cfg.poll_seconds)

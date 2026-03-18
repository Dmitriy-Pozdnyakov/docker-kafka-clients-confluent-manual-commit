#!/usr/bin/env python3
"""
Oracle LogMiner (archived logs) -> Kafka, Docker/env-friendly entrypoint.

Структура намеренно синхронизирована с jupyter-вариантом:
- конфиг через dataclass `Config`;
- один батч через `run_once(config)`;
- entrypoint `main()` работает в one-shot режиме (для cron/docker compose run);
- подробные комментарии по шагам.
"""

import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
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
    call_timeout_ms: int = 30000  # timeout Oracle-запросов в миллисекундах
    archive_log_limit: int = 3  # сколько последних archived logs подключать к LogMiner
    use_no_rowid_in_stmt: bool = True  # добавлять ли NO_ROWID_IN_STMT в options LogMiner

    # =========================
    # Kafka connection
    # =========================
    kafka_broker: str = "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"  # bootstrap servers
    kafka_topic: str = "oracle.logminer.raw"  # целевой topic
    kafka_security_protocol: str = "PLAINTEXT"  # PLAINTEXT / SSL / SASL_SSL / SASL_PLAINTEXT
    ssl_cafile: str = ""  # путь до CA сертификата (для SSL/SASL_SSL)
    ssl_check_hostname: bool = True  # проверка hostname в TLS-сертификате
    kafka_sasl_mechanism: str = "PLAIN"  # SASL механизм
    kafka_sasl_username: str = ""  # SASL username
    kafka_sasl_password: str = ""  # SASL password
    kafka_client_id: str = "oracle-logminer-archivelog-producer"  # client.id producer
    kafka_flush_timeout_sec: int = 30  # timeout flush producer
    produce_retry_timeout_sec: int = 60  # максимум ожидания при queue full на 1 сообщение
    produce_retry_poll_sec: float = 0.2  # интервал poll при queue full
    topic_per_table: bool = False  # писать ли в отдельный topic на каждую таблицу
    topic_prefix: str = ""  # префикс для topic per table, например "oracle.cdc"
    topic_separator: str = "."  # разделитель частей topic name

    # =========================
    # State management
    # =========================
    state_file: str = "./oracle_kafka_state_archivelog.json"  # state-файл с last_commit_scn
    start_from_commit_scn: int = 0  # стартовый commit_scn если state-файл отсутствует

    # =========================
    # Loop mode
    # =========================
    poll_seconds: int = 60  # legacy параметр: в one-shot режиме контейнера не используется

    # =========================
    # Data filters
    # =========================
    filter_schema: str = ""  # одиночный фильтр схемы (legacy)
    filter_table: str = ""  # одиночный фильтр таблицы (legacy)
    filter_schemas: Tuple[str, ...] = ()  # список схем
    filter_tables: Tuple[str, ...] = ()  # список таблиц
    max_rows_per_batch: int = 5000  # максимум строк за один батч (0 = без лимита)

    # =========================
    # Logging
    # =========================
    verbose: bool = True  # включить подробные логи
    log_first_n_events: int = 3  # сколько первых событий батча логировать детально

def _log(cfg: Config, message: str) -> None:
    # Единый формат логов: печатаем только при verbose.
    if cfg.verbose:
        print(f"[oracle->kafka:archivelog] {message}")


def _parse_comma_separated_upper_list(raw: str) -> List[str]:
    # Разбираем строку "A,B,C" -> ["A", "B", "C"] c trim + upper + dedupe.
    values: List[str] = []
    for item in raw.split(","):
        normalized = item.strip().upper()
        if normalized and normalized not in values:
            values.append(normalized)
    return values


def _merge_name_filters(single_value: str, many_values: Sequence[str]) -> List[str]:
    # Объединяем legacy одиночное значение и список, удаляя дубли.
    merged: List[str] = []
    single = str(single_value).strip().upper()
    if single:
        merged.append(single)
    for value in many_values:
        normalized = str(value).strip().upper()
        if normalized and normalized not in merged:
            merged.append(normalized)
    return merged


def _str_to_bool(value: str, default: bool) -> bool:
    raw = value.strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    return default


def load_config_from_env() -> Config:
    # Загружаем все настройки из env для docker-режима.
    broker = os.getenv("KAFKA_BROKER", "").strip()
    if not broker:
        broker = os.getenv("BROKER", "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094").strip()

    start_from_commit_scn = int(os.getenv("START_FROM_SCN", "0") or "0")
    filter_schemas = tuple(_parse_comma_separated_upper_list(os.getenv("FILTER_SCHEMAS", "")))
    filter_tables = tuple(_parse_comma_separated_upper_list(os.getenv("FILTER_TABLES", "")))

    return Config(
        oracle_user=os.getenv("ORACLE_USER", "").strip(),
        oracle_password=os.getenv("ORACLE_PASSWORD", "").strip(),
        oracle_dsn=os.getenv("ORACLE_DSN", "").strip(),
        call_timeout_ms=int(os.getenv("CALL_TIMEOUT_MS", "30000")),
        archive_log_limit=int(os.getenv("ARCHIVE_LOG_LIMIT", "3")),
        use_no_rowid_in_stmt=_str_to_bool(os.getenv("USE_NO_ROWID_IN_STMT", "true"), True),
        kafka_broker=broker,
        kafka_topic=os.getenv("KAFKA_TOPIC", "oracle.logminer.raw").strip(),
        kafka_security_protocol=os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT").upper(),
        ssl_cafile=os.getenv("SSL_CAFILE", "").strip(),
        ssl_check_hostname=_str_to_bool(os.getenv("SSL_CHECK_HOSTNAME", "true"), True),
        kafka_sasl_mechanism=os.getenv("KAFKA_SASL_MECHANISM", "PLAIN").strip(),
        kafka_sasl_username=os.getenv("KAFKA_SASL_USERNAME", "").strip(),
        kafka_sasl_password=os.getenv("KAFKA_SASL_PASSWORD", "").strip(),
        kafka_client_id=os.getenv("KAFKA_CLIENT_ID", "oracle-logminer-archivelog-producer").strip(),
        kafka_flush_timeout_sec=int(os.getenv("KAFKA_FLUSH_TIMEOUT_SEC", "30")),
        produce_retry_timeout_sec=int(os.getenv("PRODUCE_RETRY_TIMEOUT_SEC", "60")),
        produce_retry_poll_sec=float(os.getenv("PRODUCE_RETRY_POLL_SEC", "0.2")),
        topic_per_table=_str_to_bool(os.getenv("TOPIC_PER_TABLE", "false"), False),
        topic_prefix=os.getenv("TOPIC_PREFIX", "").strip(),
        topic_separator=os.getenv("TOPIC_SEPARATOR", ".").strip() or ".",
        state_file=os.getenv("STATE_FILE", "./oracle_kafka_state_archivelog.json").strip(),
        start_from_commit_scn=start_from_commit_scn,
        poll_seconds=int(os.getenv("POLL_SECONDS", "60")),
        filter_schema=os.getenv("FILTER_SCHEMA", "").strip(),
        filter_table=os.getenv("FILTER_TABLE", "").strip(),
        filter_schemas=filter_schemas,
        filter_tables=filter_tables,
        max_rows_per_batch=int(os.getenv("MAX_ROWS_PER_BATCH", "5000")),
        verbose=_str_to_bool(os.getenv("VERBOSE", "true"), True),
        log_first_n_events=int(os.getenv("LOG_FIRST_N_EVENTS", "3")),
    )


def validate_config(cfg: Config) -> None:
    # Валидируем обязательные поля до запуска.
    missing: List[str] = []
    if not cfg.oracle_user:
        missing.append("ORACLE_USER")
    if not cfg.oracle_password:
        missing.append("ORACLE_PASSWORD")
    if not cfg.oracle_dsn:
        missing.append("ORACLE_DSN")
    if not cfg.kafka_broker:
        missing.append("KAFKA_BROKER or BROKER")
    if missing:
        raise RuntimeError(f"Missing required env vars: {', '.join(missing)}")


def _load_state(cfg: Config) -> Dict[str, int]:
    # Загружаем last_commit_scn из state-файла, если он есть.
    path = Path(cfg.state_file)
    if path.exists():
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    return {"last_commit_scn": cfg.start_from_commit_scn}


def _save_state(cfg: Config, last_commit_scn: int) -> None:
    # Сохраняем новый watermark после успешного батча.
    path = Path(cfg.state_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "last_commit_scn": last_commit_scn,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            },
            f,
        )


def _build_kafka_producer(cfg: Config) -> Producer:
    # Собираем конфиг Producer из Config.
    proto = cfg.kafka_security_protocol.upper()
    conf: Dict[str, Any] = {
        "bootstrap.servers": cfg.kafka_broker,
        "client.id": cfg.kafka_client_id,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 10,
        "security.protocol": proto,
    }

    if proto in {"SSL", "SASL_SSL"}:
        if cfg.ssl_cafile:
            conf["ssl.ca.location"] = cfg.ssl_cafile
        conf["ssl.endpoint.identification.algorithm"] = (
            "https" if cfg.ssl_check_hostname else "none"
        )

    if proto in {"SASL_SSL", "SASL_PLAINTEXT"}:
        conf["sasl.mechanism"] = cfg.kafka_sasl_mechanism
        conf["sasl.username"] = cfg.kafka_sasl_username
        conf["sasl.password"] = cfg.kafka_sasl_password

    return Producer(conf)


def _end_logminer(cur: oracledb.Cursor) -> None:
    # Безопасно завершаем текущую сессию LogMiner.
    cur.execute(
        """
        BEGIN
          DBMS_LOGMNR.END_LOGMNR;
        EXCEPTION
          WHEN OTHERS THEN NULL;
        END;
        """
    )


def _get_archived_logs(cur: oracledb.Cursor, limit_rows: int) -> List[str]:
    # Берем N последних archived log файлов и разворачиваем в хронологический порядок.
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
    cur.execute(sql, {"limit_rows": limit_rows})
    files = [r[0] for r in cur.fetchall() if r and r[0]]
    files.reverse()
    return files


def _add_logfile(cur: oracledb.Cursor, logfile: str, first: bool) -> None:
    # Первый файл добавляется как NEW, остальные как ADDFILE.
    option = "DBMS_LOGMNR.NEW" if first else "DBMS_LOGMNR.ADDFILE"
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
    # Стартуем LogMiner на выбранном наборе archived logs.
    if not log_files:
        raise RuntimeError("No archived logs found in v$archived_log")

    _end_logminer(cur)
    for idx, logfile in enumerate(log_files):
        _add_logfile(cur, logfile, first=(idx == 0))

    options = "DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG + DBMS_LOGMNR.COMMITTED_DATA_ONLY"
    if cfg.use_no_rowid_in_stmt:
        options += " + DBMS_LOGMNR.NO_ROWID_IN_STMT"

    cur.call_timeout = cfg.call_timeout_ms
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
    filter_schemas: Sequence[str],
    filter_tables: Sequence[str],
    max_rows_per_batch: int,
) -> List[Dict[str, Any]]:
    # Базовый SQL: только DML после watermark commit_scn.
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
    binds: Dict[str, Any] = {"from_commit_scn": from_commit_scn}

    if filter_schemas:
        schema_placeholders = []
        for idx, schema_name in enumerate(filter_schemas):
            bind_name = f"filter_schema_{idx}"
            schema_placeholders.append(f":{bind_name}")
            binds[bind_name] = schema_name
        sql += f" AND seg_owner IN ({', '.join(schema_placeholders)})"

    if filter_tables:
        table_placeholders = []
        for idx, table_name in enumerate(filter_tables):
            bind_name = f"filter_table_{idx}"
            table_placeholders.append(f":{bind_name}")
            binds[bind_name] = table_name
        sql += f" AND table_name IN ({', '.join(table_placeholders)})"

    sql += " ORDER BY commit_scn, sequence#, rs_id, ssn"

    if max_rows_per_batch > 0:
        sql = f"SELECT * FROM ({sql}) WHERE ROWNUM <= :max_rows_per_batch"
        binds["max_rows_per_batch"] = int(max_rows_per_batch)

    cur.execute(sql, binds)
    columns = [c[0].lower() for c in cur.description]
    rows: List[Dict[str, Any]] = []
    for record in cur:
        row = dict(zip(columns, record))
        ts = row.get("timestamp")
        if isinstance(ts, datetime):
            row["timestamp"] = ts.isoformat()
        rows.append(row)
    return rows


def _event_key(row: Dict[str, Any]) -> bytes:
    # Детерминированный key события для Kafka.
    key = (
        f"{row.get('seg_owner','')}.{row.get('table_name','')}|"
        f"{row.get('commit_scn','')}|{row.get('redo_sequence','')}|"
        f"{row.get('rs_id','')}|{row.get('ssn','')}"
    )
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
    # Безопасная отправка: queue full -> poll + retry до дедлайна.
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
            producer.poll(cfg.produce_retry_poll_sec)
            retries += 1
            waited_sec = now - started


def run_once(cfg: Config) -> Dict[str, Any]:
    # Объединяем legacy одиночные фильтры со списками.
    merged_schemas = _merge_name_filters(cfg.filter_schema, cfg.filter_schemas)
    merged_tables = _merge_name_filters(cfg.filter_table, cfg.filter_tables)

    # 1) Загружаем состояние.
    state = _load_state(cfg)
    last_commit_scn = int(state.get("last_commit_scn", cfg.start_from_commit_scn))
    _log(
        cfg,
        (
            "start batch "
            f"(topic_mode={'per_table' if cfg.topic_per_table else 'single'}, "
            f"base_topic={cfg.kafka_topic}, from_commit_scn={last_commit_scn}, "
            f"filter_schemas={','.join(merged_schemas) if merged_schemas else '*'}, "
            f"filter_tables={','.join(merged_tables) if merged_tables else '*'}, "
            f"max_rows_per_batch={cfg.max_rows_per_batch})"
        ),
    )

    # 2) Инициализация producer и счетчиков.
    producer = _build_kafka_producer(cfg)
    delivered = 0
    failed = 0
    queue_full_retries = 0
    queue_full_wait_sec = 0.0
    topics_used: Dict[str, int] = {}
    timings: Dict[str, float] = {}
    t0 = time.monotonic()

    def on_delivery(err, _msg):
        nonlocal delivered, failed
        if err is not None:
            failed += 1
        else:
            delivered += 1

    conn: Optional[oracledb.Connection] = None
    cur: Optional[oracledb.Cursor] = None
    logs: List[str] = []
    try:
        # 3) Connect to Oracle + check access.
        t_connect = time.monotonic()
        conn = oracledb.connect(user=cfg.oracle_user, password=cfg.oracle_password, dsn=cfg.oracle_dsn)
        timings["oracle_connect_sec"] = time.monotonic() - t_connect
        _log(cfg, f"connected to Oracle DSN={cfg.oracle_dsn}")

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM v$logmnr_contents WHERE 1 = 0")
        _ = cur.fetchone()
        _log(cfg, "access check to v$logmnr_contents passed")

        # 4) Выбираем archived logs и стартуем LogMiner.
        t_logs = time.monotonic()
        logs = _get_archived_logs(cur, cfg.archive_log_limit)
        timings["select_archived_logs_sec"] = time.monotonic() - t_logs
        _log(cfg, f"archived logs selected: {len(logs)}")
        for lf in logs:
            _log(cfg, f"  logfile: {lf}")

        t_logminer = time.monotonic()
        _start_logminer(cur, cfg, logs)
        timings["start_logminer_sec"] = time.monotonic() - t_logminer
        _log(cfg, "LogMiner started")

        # 5) Читаем строки из LogMiner.
        t_fetch = time.monotonic()
        rows = _fetch_rows(
            cur=cur,
            from_commit_scn=last_commit_scn,
            filter_schemas=merged_schemas,
            filter_tables=merged_tables,
            max_rows_per_batch=cfg.max_rows_per_batch,
        )
        timings["fetch_rows_sec"] = time.monotonic() - t_fetch
        _log(cfg, f"rows fetched from v$logmnr_contents: {len(rows)}")

        # 6) Публикуем в Kafka.
        t_publish = time.monotonic()
        for idx, row in enumerate(rows):
            value = json.dumps(row, ensure_ascii=False).encode("utf-8")
            topic = _resolve_topic(cfg, row)
            retries, waited = _produce_with_backpressure(
                cfg=cfg,
                producer=producer,
                topic=topic,
                key=_event_key(row),
                value=value,
                callback=on_delivery,
            )
            queue_full_retries += retries
            queue_full_wait_sec += waited
            topics_used[topic] = topics_used.get(topic, 0) + 1
            producer.poll(0)

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
        timings["publish_rows_sec"] = time.monotonic() - t_publish

        # 7) Flush и проверка ошибок delivery.
        t_flush = time.monotonic()
        producer.flush(cfg.kafka_flush_timeout_sec)
        timings["flush_sec"] = time.monotonic() - t_flush
        _log(
            cfg,
            (
                "kafka flush done "
                f"(delivered={delivered}, failed={failed}, queued={len(rows)}, "
                f"queue_full_retries={queue_full_retries}, "
                f"queue_full_wait_sec={queue_full_wait_sec:.3f})"
            ),
        )
        if failed > 0:
            raise RuntimeError(f"Kafka delivery failures: {failed} of {len(rows)}")

        # 8) Обновляем state watermark.
        if rows:
            new_commit_scn = max(int(r["commit_scn"]) for r in rows if r.get("commit_scn") is not None)
            last_commit_scn = max(last_commit_scn, new_commit_scn)
            _save_state(cfg, last_commit_scn)
            _log(cfg, f"state updated: last_commit_scn={last_commit_scn}")
        else:
            _log(cfg, "no new rows, state not changed")

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
        # 9) Гарантированный cleanup.
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


def main() -> int:
    cfg = load_config_from_env()
    validate_config(cfg)

    _log(
        cfg,
        (
            "start one-shot batch "
            f"(dsn={cfg.oracle_dsn}, topic={cfg.kafka_topic}, "
            f"archive_log_limit={cfg.archive_log_limit}, state_file={cfg.state_file})"
        ),
    )
    try:
        stats = run_once(cfg)
        _log(cfg, f"one-shot finished: {stats}")
        return 0
    except Exception as exc:
        print(f"[oracle->kafka:archivelog] ERROR: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
